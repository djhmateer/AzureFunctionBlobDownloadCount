using System;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using static System.Environment;

namespace DaveMTest
{
    public static class CopyLogs
    {
        // RunOnStartup true so debugging easier both locally and live
        [FunctionName("CopyLogs")]
        public static void Run([TimerTrigger("0 */30 * * * *", RunOnStartup = true)] TimerInfo myTimer, ILogger log)
        {
            log.LogInformation($"C# CopyLogs Time trigger function executed at: {DateTime.Now}");

            var container = CloudStorageAccount.Parse(GetEnvironmentVariable("DaveMTestVideoStorageConnectionString")).CreateCloudBlobClient().GetContainerReference("showlogs/");

            var d = DateTime.UtcNow;
            var now = DateTime.UtcNow;
            var diff = now - d;

            // check each days logs
            var days = Math.Ceiling(diff.TotalDays); // always round up to the next day too
            for (int i = 0; i <= days; i++)
            {
                var add = new TimeSpan(0 - i, 0, 0, 0);
                var date = now.Add(add);

                var blobPrefix = "$logs" + "/blob/" + date.Year + "/" + date.Month.ToString("D2") + "/" + date.Day.ToString("D2") + "/";
                log.LogInformation($"Scanning:  {blobPrefix}");

                //https://docs.microsoft.com/en-us/azure/visual-studio/vs-storage-aspnet-core-getting-started-blobs
                var resultSegment = CloudStorageAccount.Parse(GetEnvironmentVariable("DaveMTestVideoStorageConnectionString"))
                    .CreateCloudBlobClient().ListBlobsSegmentedAsync(blobPrefix, true, BlobListingDetails.None, null, null, null, null).Result;
                foreach (var item in resultSegment.Results)
                {
                    if (item.GetType() == typeof(CloudBlockBlob))
                    {
                        var blob = (CloudBlockBlob)item;
                        var exists = BlobExists(CloudStorageAccount.Parse(GetEnvironmentVariable("DaveMTestVideoStorageConnectionString")).CreateCloudBlobClient(), "showlogs/", blob.Name);
                        if (exists) { } //log.LogInformation("Blob exists in destination already");
                        else
                        {
                            var sharedAccessUri = GetContainerSasUri(blob);
                            var sourceBlob = new CloudBlockBlob(new Uri(sharedAccessUri));

                            //copy it
                            var targetBlob = container.GetBlockBlobReference(blob.Name);
                            var x = targetBlob.StartCopyAsync(sourceBlob).Result;
                            log.LogInformation("copy done");
                        }
                    }
                }
                // write out the current time to the sync point 
                //outputBlob.Write(DateTime.UtcNow.ToString("O"));
            }
            log.LogInformation($"C# CopyLogs Time trigger finished: {DateTime.Now}");
        }

        public static bool BlobExists(CloudBlobClient client, string containerName, string key)
        {
            return client.GetContainerReference(containerName)
                .GetBlockBlobReference(key)
                .ExistsAsync().Result;
        }

        static string GetContainerSasUri(CloudBlockBlob blob)
        {
            var sasConstraints = new SharedAccessBlobPolicy
            {
                SharedAccessStartTime = DateTime.UtcNow.AddMinutes(-5),
                SharedAccessExpiryTime = DateTime.UtcNow.AddHours(24),
                Permissions = SharedAccessBlobPermissions.Read
            };

            //Return the URI string for the container, including the SAS token.
            return blob.Uri + blob.GetSharedAccessSignature(sasConstraints);
        }
    }
}
