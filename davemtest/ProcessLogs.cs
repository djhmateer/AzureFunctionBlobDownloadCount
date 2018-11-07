using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using CsvHelper;
using CsvHelper.Configuration;
using Dapper;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using static System.Environment;

namespace DaveMTest
{
    public static class ProcessLogs
    {
        const string BlobConnectionString = "DaveMTestVideoStorageConnectionString"; // suffix ConnectionString important below

        [FunctionName("ProcessLogs")]
        public static void Run([BlobTrigger("showlogs/{name}", Connection = BlobConnectionString)]Stream blobStream, string name, ILogger log)
        {
            log.LogInformation($"ProcessLogs:{name} \n Size: {blobStream.Length} Bytes");
            // get the blob as a string https://stackoverflow.com/a/17801826/26086
            blobStream.Seek(0, SeekOrigin.Begin);
            var reader = new StreamReader(blobStream);
            var blob = reader.ReadToEnd();

            using (TextReader sr = new StringReader(blob))
            {
                var csv = new CsvReader(sr);
                csv.Configuration.Delimiter = ";";
                csv.Configuration.RegisterClassMap<LogEntryMap>();
                csv.Configuration.HasHeaderRecord = false;
                var records = csv.GetRecords<LogEntry>().ToList();
                records = FilterRecords(records);
                SendToDB(records, log);
            }
        }
        public static List<LogEntry> FilterRecords(List<LogEntry> entries)
        {
            return entries.Where(e =>
                e.RequestURL.EndsWith("mp4")
                && e.RESTOperationType == "GetBlob"
                ).ToList();
        }

        public static void SendToDB(List<LogEntry> entries, ILogger log)
        {
            using (var connection = new SqlConnection(GetEnvironmentVariable("DaveMTestDBConnectionString")))
            {
                connection.Open();
                foreach (var e in entries)
                {
                    var sql = "Insert VideoDownloadLog ([TransactionDateTime],[OperationType]," +
                        "[ObjectKey],[UserAgent],[Referrer],[FileName],[DeviceType],[IPAddress],[RequestID], RequestStatus) " +
                        "VALUES (@val1,@val2,@val3,@val4,@val5,@val6,@val7,@val8,@val9,@val10)";

                    // open a transaction in case multiple queries happening at same time on multiple instances of azure function
                    SqlTransaction transaction = connection.BeginTransaction();
                    using (var cmd = new SqlCommand(sql, connection, transaction))
                    {
                        cmd.Parameters.AddWithValue("@val1", e.TransactionStartTime);
                        cmd.Parameters.AddWithValue("@val2", e.RESTOperationType);
                        cmd.Parameters.AddWithValue("@val3", e.ObjectKey);
                        cmd.Parameters.AddWithValue("@val4", new string(e.UserAgent.Take(255).ToArray()));
                        cmd.Parameters.AddWithValue("@val5", new string(e.Referrer.Take(1024).ToArray()));
                        // 9 value takes into account / /videos/ - as the name of the blob storage can change (when testing)
                        var blobConnectionName = BlobConnectionString.Replace("ConnectionString", "");
                        var filename = e.ObjectKey.Substring(blobConnectionName.Length + 9,
                            e.ObjectKey.Length - (blobConnectionName.Length + 9));
                        cmd.Parameters.AddWithValue("@val6", filename);
                        var deviceType = ParseDeviceType(e.UserAgent);
                        cmd.Parameters.AddWithValue("@val7", deviceType);
                        var ipaddress = e.ClientIP.Contains(":")
                            ? e.ClientIP.Substring(0, e.ClientIP.IndexOf(':'))
                            : e.ClientIP; // removing the port
                        cmd.Parameters.AddWithValue("@val8", ipaddress);
                        cmd.Parameters.AddWithValue("@val9", e.RequestID);
                        cmd.Parameters.AddWithValue("@val10", e.RequestStatus);

                        // has there been a duplicate record already inserted in 10second window
                        var startWindow = e.TransactionStartTime.AddSeconds(-5);
                        var endWindow = e.TransactionStartTime.AddSeconds(5);

                        var result = connection.Query<int>(
                            "Select Count(*) From VideoDownloadLog Where Filename = @Filename And TransactionDateTime > @StartWindow" +
                            " And TransactionDateTime < @EndWindow And IPAddress = @IPAddress",
                            new
                            {
                                Filename = filename,
                                StartWindow = startWindow,
                                EndWindow = endWindow,
                                IPAddress = ipaddress
                            }, transaction // pass dapper the transaction
                        ).FirstOrDefault();

                        if (result == 0)
                        {
                            var rows = cmd.ExecuteNonQuery();
                            if (rows > 0) log.LogInformation($"New download found and inserted: {e.RequestID}");
                        }
                        else if (deviceType == "ApacheBench") // for perf quality testing to see if it picks up all downloads
                        {
                            var rows = cmd.ExecuteNonQuery();
                            if (rows > 0)
                                log.LogInformation(
                                    $"ApacheBench New download found and inserted: {e.RequestID}");
                        }
                        else log.LogInformation("Duplicate download found");
                        transaction.Commit();
                    }
                }
            }
        }

        // TODO make this more solid and up to date
        public static string ParseDeviceType(string userAgent)
        {
            var x = userAgent.ToLower();
            if (x.Contains("iphone") && x.Contains("mozilla")) return "iPhone Chrome";
            if (x.Contains("firefox")) return "Firefox";
            if (x.Contains("edge")) return "Edge";
            if (x.Contains("chrome")) return "Chrome";

            if (userAgent.ToLower().Contains("stitcher")) return "Stitcher";
            if (userAgent.ToLower().Contains("overcast")) return "Overcast";
            if (userAgent.ToLower().Contains("windows mobile")) return "Windows Mobile";
            if (userAgent.ToLower().Contains("windows phone")) return "Windows Phone";
            if (userAgent.ToLower().Contains("apple tv")) return "Apple TV";
            if (userAgent.ToLower().Contains("itunes")) return "iTunes";
            if (userAgent.ToLower().Contains("feedreader")) return "Feedreader";
            if (userAgent.ToLower().Contains("windows nt")) return "Windows";
            if (userAgent.ToLower().Contains("mozilla")) return "Mozilla";
            if (userAgent.ToLower().Contains("zune")) return "Zune";
            if (userAgent.ToLower().Contains("pocket casts")) return "Pocket Casts";
            if (userAgent.ToLower().Contains("android")) return "Android";
            if (userAgent.ToLower().Contains("iphone")) return "iPhone";
            if (userAgent.ToLower().Contains("ipad")) return "iPad";
            if (userAgent.ToLower().Contains("ipod")) return "iPod";
            if (userAgent.ToLower().Contains("ios")) return "iOS";

            string other;

            if (userAgent.Contains(@"/"))
                other = userAgent.Substring(0, userAgent.IndexOf(@"/"));
            else
                other = new string(userAgent.Take(40).ToArray());

            return other;
        }

        public class LogEntry
        {
            public string LogVersion { get; set; }
            public DateTime TransactionStartTime { get; set; }
            public string RESTOperationType { get; set; }
            public string RequestStatus { get; set; }
            public string HTTPStatusCode { get; set; }
            public string E2ELatency { get; set; }
            public string ServerLatency { get; set; }
            public string AuthenticationType { get; set; }
            public string RequestorAccountName { get; set; }
            public string OwnerAccountName { get; set; }
            public string ServiceType { get; set; }
            public string RequestURL { get; set; }
            public string ObjectKey { get; set; }
            public string RequestID { get; set; }
            public string OperationNumber { get; set; }
            public string ClientIP { get; set; }
            public string RequestVersion { get; set; }
            public string RequestHeaderSize { get; set; }
            public string RequestPacketSize { get; set; }
            public string ResponseHeaderSize { get; set; }
            public string ResponsePacketSize { get; set; }
            public string RequestContentLength { get; set; }
            public string RequestMD5 { get; set; }
            public string ServerMD5 { get; set; }
            public string ETag { get; set; }
            public string LastModifiedTime { get; set; }
            public string ConditionsUsed { get; set; }
            public string UserAgent { get; set; }
            public string Referrer { get; set; }
            public string ClientRequestID { get; set; }

        }

        // Logfile line that is written when a Blob is downloaded
        public sealed class LogEntryMap : ClassMap<LogEntry>
        {
            public LogEntryMap()
            {
                Map(m => m.LogVersion).Index(0);
                Map(m => m.TransactionStartTime).Index(1);
                Map(m => m.RESTOperationType).Index(2);
                Map(m => m.RequestStatus).Index(3);
                Map(m => m.HTTPStatusCode).Index(4);
                Map(m => m.E2ELatency).Index(5);
                Map(m => m.ServerLatency).Index(6);
                Map(m => m.AuthenticationType).Index(7);
                Map(m => m.RequestorAccountName).Index(8);
                Map(m => m.OwnerAccountName).Index(9);
                Map(m => m.ServiceType).Index(10);
                Map(m => m.RequestURL).Index(11);
                Map(m => m.ObjectKey).Index(12);
                Map(m => m.RequestID).Index(13);
                Map(m => m.OperationNumber).Index(14);
                Map(m => m.ClientIP).Index(15);
                Map(m => m.RequestVersion).Index(16);
                Map(m => m.RequestHeaderSize).Index(17);
                Map(m => m.RequestPacketSize).Index(18);
                Map(m => m.ResponseHeaderSize).Index(19);
                Map(m => m.ResponsePacketSize).Index(20);
                Map(m => m.RequestContentLength).Index(21);
                Map(m => m.RequestMD5).Index(22);
                Map(m => m.ServerMD5).Index(23);
                Map(m => m.ETag).Index(24);
                Map(m => m.LastModifiedTime).Index(25);
                Map(m => m.ConditionsUsed).Index(26);
                Map(m => m.UserAgent).Index(27);
                Map(m => m.Referrer).Index(28);
                Map(m => m.ClientRequestID).Index(29);
            }
        }
    }
}
