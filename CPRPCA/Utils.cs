using System;
using System.Data.SqlClient;
using System.Timers;
using System.IO;
using System.Configuration;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading;

using SharpCompress.Archives.GZip;
using SharpCompress.Archives;
using SharpCompress.Common;
using System.Text;
using Newtonsoft.Json;

using System.Runtime.InteropServices;

namespace CPREnvoy
{
    public static class ECPRERROR
    {
        public static string SqlExceptionDetail(SqlException sqlExcept)
        {
            StringBuilder errorMessage = new StringBuilder();

            for (int i=0; i<sqlExcept.Errors.Count; i++)
            {
                errorMessage.Append(String.Format("[Error Message: {0} | Error Number: {1} | Line Number: {2} | Source: {3} | Store Procedure: {4}]",
                    sqlExcept.Errors[i].Message,
                    sqlExcept.Errors[i].Number,
                    sqlExcept.Errors[i].LineNumber,
                    sqlExcept.Errors[i].Source,
                    sqlExcept.Errors[i].Procedure));
            }

            return errorMessage.ToString();
        }

        public static string ExceptionDetail(Exception except)
        {
            var errorMessage = String.Format("Exception Type: {0} | Exception Message: {1} | StackTrace: {2}",
                except.GetType().FullName,
                except.Message,
                except.StackTrace);

            return errorMessage;
        }
    }

    public static class NetworkMonitoring
    {
        [DllImport("wininet.dll")]
        private extern static bool InternetGetConnectedState(out int desc, int res);

        public static bool IsInternetAvailable()
        {
            int desc;
            return InternetGetConnectedState(out desc, 0);
        }
    }

    public static class Utils
    {
        private static string tmpFileWindowsRestart = Path.Combine(Path.GetTempPath(), ConfigurationManager.AppSettings["ecpr_service_name"].Replace(" ",""));
        public static bool isReconnecting = false;

        public static void maybe_reconnect(object self, ref SqlConnection sqlConnection, int try_again_after, Logger logger)
        {
            string connectionError = "Had an error while trying to connect to SQL SERVER ({0}). Will try again after {1} secs.";

            try
            {
                using (SqlCommand sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = "select 1";
                    sqlCommand.CommandType = CommandType.Text;
                    sqlCommand.Notification = null;
                    sqlCommand.CommandTimeout = try_again_after * 1000;
                    sqlCommand.ExecuteScalar();
                    isReconnecting = false;
                }
            }
            catch (SqlException error)
            {
                try
                {
                    SqlConnection.ClearPool(sqlConnection);
                }
                catch (Exception) { }

                try
                {
                    sqlConnection.Close();
                }catch(Exception) { }

                logger.write(String.Format(connectionError, error.Message, try_again_after), Logger.LOGLEVEL.ERROR);

                try
                {
                    sqlConnection.Open();
                }
                catch (Exception) { }

                isReconnecting = true;
            }
            catch (System.InvalidOperationException)
            {
                try
                {
                    sqlConnection.Open();
                }
                catch (SqlException error)
                {
                    logger.write(String.Format(connectionError, error.Message, try_again_after), Logger.LOGLEVEL.ERROR);
                    isReconnecting = true;
                }
                catch (Exception) { }
            }
        }

        public static bool try_connect_to_db(string connectionString, out string errorMessage)
        {
            errorMessage = String.Empty;

            using (SqlConnection sqlConnection = new SqlConnection(connectionString))
            {
                try
                {
                    sqlConnection.Open();
                }
                catch (SqlException error)
                {
                    errorMessage = error.Message;
                    return false;
                }
            }

            return true;
        }

        public static string getUnixTimeStamp()
        {
            var timeSpan = (DateTime.UtcNow - new DateTime(1970, 1, 1, 0, 0, 0));
            return String.Format("{0}", (long)timeSpan.TotalSeconds);
        }

        public static void windowsRestarting()
        {
            try
            {
                using (File.Create(Utils.tmpFileWindowsRestart))
                {

                }
            }
            catch (Exception) { }
        }

        public static bool isStartingFromWindowsRestart()
        {
            try
            {
                if (File.Exists(Utils.tmpFileWindowsRestart))
                {
                    File.Delete(Utils.tmpFileWindowsRestart);
                    return true;
                }
            }
            catch (Exception) { }

            return false;
        }
    }

    public static class TableDumper

    {
        public static void QueryToFile(SqlConnection connection, string sqlQuery, string csvFilename, string customerCodeEnv, int commandTimeout)
        {
            string fileName = String.Format("{0}.csv", csvFilename);
            string destinationFile = Path.Combine(Path.GetTempPath(), fileName);

            using (var command = new SqlCommand(sqlQuery, connection))
            {
                command.CommandTimeout = commandTimeout;
                using (var reader = command.ExecuteReader())
                using (var outFile = File.CreateText(destinationFile))
                {
                    string[] columnNames = GetColumnNames(reader).ToArray();
                    int numFields = columnNames.Length;

                    outFile.WriteLine(string.Join(",", columnNames));
                    if (reader.HasRows)
                    {
                        while (reader.Read())
                        {
                            string[] columnValues =
                                Enumerable.Range(0, numFields)
                                          .Select(i => Encoding.UTF8.GetString(Encoding.UTF8.GetBytes(reader.GetValue(i).ToString())))
                                          .Select(field => string.Concat("\"", field.Replace("\"", "\"\""), "\""))
                                          .ToArray();
                            outFile.WriteLine(string.Join(",", columnValues));
                        }
                    }
                }
            }

            CompressFile(fileName, destinationFile);

            S3Uploader.upload(destinationFile + ".gz", String.Format("{0}/{1}.gz", customerCodeEnv, fileName), "ecpr");
        }

        public static void DumpTableToFile_JSON(SqlConnection connection, string tableName, string fieldsName, string filterCond, string customerCodeEnv, int commandTimeout)
        {
            string fileName = String.Format("{0}.json", tableName.ToLower());
            string destinationFile = Path.Combine(Path.GetTempPath(), fileName);
            string whereCond = "";

            if (filterCond != null)
                whereCond = String.Format(" WHERE {0}", filterCond);

            SqlTransaction trans;

            trans = connection.BeginTransaction(IsolationLevel.ReadUncommitted);

            using (var command = new SqlCommand("SELECT " + fieldsName + " FROM " + tableName + whereCond, connection))
            {
                command.CommandTimeout = commandTimeout;
                command.Transaction = trans;

                TableDumper.readerToFile_JSON(command, destinationFile);
            }

            CompressFile(fileName, destinationFile);

            S3Uploader.upload(destinationFile + ".gz", String.Format("{0}/{1}.gz", customerCodeEnv, fileName), "ecpr");
        }

        public static void DumpTableToFile(SqlConnection connection, string tableName, string fieldsName, string filterCond, string customerCodeEnv, int commandTimeout)
        {
            string fileName = String.Format("{0}.csv", tableName.ToLower());
            string destinationFile = Path.Combine(Path.GetTempPath(), fileName);
            string whereCond = "";

            if (filterCond != null)
                whereCond = String.Format(" WHERE {0}", filterCond);

            SqlTransaction trans;

            trans = connection.BeginTransaction(IsolationLevel.ReadUncommitted);

            using (var command = new SqlCommand("SELECT " + fieldsName + " FROM " + tableName + whereCond, connection))
            {
                command.CommandTimeout = commandTimeout;
                command.Transaction = trans;

                TableDumper.readerToFile(command, destinationFile);
            }

            CompressFile(fileName, destinationFile);

            S3Uploader.upload(destinationFile + ".gz", String.Format("{0}/{1}.gz", customerCodeEnv, fileName), "ecpr");
        }

        public static void runQueryToFile(SqlConnection connection, string runQuery, string fileName, string customerCodeEnv, int commandTimeout, string resultType)
        {
            string destinationFile = Path.Combine(Path.GetTempPath(), fileName);

            using (var command = new SqlCommand(runQuery, connection))
            {
                command.CommandTimeout = commandTimeout;

                if (resultType == "csv")
                    TableDumper.readerToFile(command, destinationFile);
                else
                    TableDumper.readerToFile_JSON(command, destinationFile);
            }

            CompressFile(fileName, destinationFile);

            S3Uploader.upload(destinationFile + ".gz", String.Format("{0}/{1}.gz", customerCodeEnv, fileName), "ecpr");
        }

        private static void readerToFile_JSON(SqlCommand command, string destinationFile)
        {
            using (var reader = command.ExecuteReader())
            using (StreamWriter file = File.CreateText(destinationFile))
            using (JsonTextWriter writer = new JsonTextWriter(file))
            {
                if (reader.HasRows)
                {
                    string[] columnNames = GetColumnNames(reader).ToArray();

                    //writer.Formatting = Formatting.Indented;
                    writer.WriteStartArray();
                    {
                        while (reader.Read())
                        {
                            writer.WriteStartObject();
                            {
                                foreach (string columnName in columnNames)
                                {
                                    writer.WritePropertyName(columnName.ToLower());
                                    writer.WriteValue(reader[columnName]);
                                }
                            }
                            writer.WriteEndObject();
                        }
                    }

                    writer.WriteEndArray();
                }
            }
        }

        private static void readerToFile(SqlCommand command, string destinationFile)
        {
            using (var reader = command.ExecuteReader())
            using (var outFile = File.CreateText(destinationFile))
            {
                string[] columnNames = GetColumnNames(reader).ToArray();
                int numFields = columnNames.Length;

                outFile.WriteLine(string.Join(",", columnNames));
                if (reader.HasRows)
                {
                    while (reader.Read())
                    {
                        string[] columnValues =
                            Enumerable.Range(0, numFields)
                                      .Select(i => Encoding.UTF8.GetString(Encoding.UTF8.GetBytes(reader.GetValue(i).ToString())))
                                      .Select(field => string.Concat("\"", field.Replace("\"", "\"\""), "\""))
                                      .ToArray();
                        outFile.WriteLine(string.Join(",", columnValues));
                    }
                }
            }
        }

        private static void CompressFile(string fileName, string destinationFile)
        {
           using (var archive = GZipArchive.Create())
            {
                archive.AddAllFromDirectory(Path.GetTempPath(), fileName);
                archive.SaveTo(destinationFile + ".gz", CompressionType.Deflate);
            }
        }

        private static IEnumerable<string> GetColumnNames(IDataReader reader)
        {
            foreach (DataRow row in reader.GetSchemaTable().Rows)
            {
                yield return (string)row["ColumnName"];
            }
        }

    }
}
