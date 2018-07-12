using System;
using System.Collections.Generic;
using System.Text;
using Newtonsoft.Json;
using System.Data.SqlClient;
using System.Data;
using System.Globalization;
using Newtonsoft.Json.Linq;

namespace CPREnvoy
{
    public partial class FullSyncControlCenter : AMQPControlCenter
    {
        public FullSyncControlCenter(Config eConfig, Logger logger)
            : base(eConfig, logger, "sync")
        {
        }
    }

    public partial class SyncControlCenter : AMQPControlCenter
    {
        private Logger logger;
        private Config config;
        private int sqlCommandTimeout;
        private int try_again_after;

        private string dateTimeFormat = "MM/dd/yyyy HH:mm";
        private string lastQueryErrorMessage = String.Empty;

        public SyncControlCenter(Config eConfig, Logger logger)
            :base(eConfig, logger, "sync")
        {
            this.logger = logger;
            this.config = eConfig;
            this.sqlCommandTimeout = eConfig.defaultSQLQueryTimeout;
            this.try_again_after = eConfig.try_again_after;

            if (eConfig.radius_only_customer)
            {
                this.logger.write( "This is Radius only customer. No need to subscribe to ebridge-cud.", Logger.LOGLEVEL.INFO);
                return;
            }

            this.consumerCallback += (model, args) =>
            {
                var message = Encoding.UTF8.GetString(args.Body);
                ulong deliveryTag = args.DeliveryTag;

                dynamic deserializeObject = null;

                try
                {
                    message = message.Replace("'", "''");
                    deserializeObject = JsonConvert.DeserializeObject(message);
                }
                catch (Exception e)
                {
                    logger.write( String.Format("Cannot parse the message {0} - Error: {0}", message, e.ToString()), Logger.LOGLEVEL.ERROR);
                    this.consumerBasicACK(deliveryTag);
                }

                try
                {
                    if (deserializeObject != null)
                    {
                        using (SqlConnection _sqlConnection = new SqlConnection(this.config.cprDatabaseConnectionString))
                        {
                            try
                            {
                                if (_sqlConnection.State != ConnectionState.Open)
                                    _sqlConnection.Open();
                            }
                            catch (Exception) { }

                            using(SqlCommand sqlCommand = _sqlConnection.CreateCommand())
                            {
                                sqlCommand.CommandText = "select 1";
                                sqlCommand.CommandType = CommandType.Text;
                                sqlCommand.CommandTimeout = try_again_after * 1000;
                                sqlCommand.ExecuteScalar();

                                this.receiveMessage(deserializeObject, deliveryTag);
                                this.consumerBasicACK(deliveryTag);
                            }
                        }
                    }
                }
                catch (Exception)
                {
                    this.consumerBasicNACK(deliveryTag);
                }
            };

            this.consumerInit();
        }

        private void syncError(string error_message, dynamic jsonData)
        {
            logger.write( String.Format("{0}. Please review the message: {1}", error_message, jsonData), Logger.LOGLEVEL.ERROR);
        }

        public void sendMessageByKey(string pKey)
        {
            string jsonData = (string)DataChangesCache.GetItem(pKey);

            if (jsonData == null)
                return;

            DataChangesCache.setPOSTing(pKey);

            if (pKey.Contains("ECPR_CUD_ID:")) // e.g.: ECPR_CUD_ID:<table_name>:1
            {
                string[] ecpr_vals = pKey.Split(':');
                int ecpr_status = 0;

                if (this.publishMessage(jsonData))
                {
                    // set ecpr_id.ecpr_status = 2 for the next delete.
                    Console.WriteLine("SENT SUCCESSFULLY, SET ecpr_status=2 FOR ecpr_id={0} TABLE ecpr_{1}_logs", ecpr_vals[2], ecpr_vals[1]);
                    ecpr_status = 2;
                } else
                {
                    Console.WriteLine("SENDING FAILED, WILL TRY AGAIN!");
                    Console.WriteLine(jsonData);
                    // this.logger.write( "Sending CUD message failed, will try again later !", Logger.LOGLEVEL.ERROR);
                    // set ecpr_id.ecpr_status = 0 for the next retry.
                    Console.WriteLine("SENT FAILED, SET ecpr_status=0 FOR ecpr_id={0} TABLE ecpr_{1}_logs", ecpr_vals[2], ecpr_vals[1]);
                }

                try
                {
                    this.executeNonQuery(String.Format("UPDATE [dbo].[ecpr_{0}_logs] SET ecpr_status={1} WHERE ecpr_id={2}", ecpr_vals[1], ecpr_status, ecpr_vals[2]));
                    DataChangesCache.RemoveItem(pKey);
                } catch (Exception error)
                {
#if DEBUG
                    Console.WriteLine(error);
#endif
                }
            }
            else
            {
                if (this.publishMessage(jsonData))
                {
                    DataChangesCache.RemoveItem(pKey);
                }
                else
                {
                    this.logger.write( "Sending CONFLICT/ERROR message failed, will try again later !", Logger.LOGLEVEL.ERROR);
                }
            }

            DataChangesCache.removePOSTing(pKey);
        }

        private void receiveMessage(dynamic jsonData, ulong deliveryTag)
        {
            /* Receive all messages from ebridge and check if it have
             * data conflict before do update.
             *
             * for delete:
             * {
             *   "type": "delete",
             *   "source": "HR",
             *   "key": "MRN",
             *   "value": 100,
             *   "uuid": "7b832c4b-e92a-4521-9c11-575a42119348"
             *  }
             *
             * for update:
             * {
             *   "type": "update",
             *   "source": "HR",
             *   "key": "MRN",
             *   "value": 100,
             *   "uuid": "7b832c4b-e92a-4521-9c11-575a42119348",
             *   "last_seen": "07/12/2015 12:30",
             *   "data": {
             *     "first_name": "Foo"
             *   }
             * }
             *
             * for insert
             * {
             *   "type": "insert",
             *   "source": "HR",
             *   "uuid": "7b832c4b-e92a-4521-9c11-575a42119348",
             *   "data": {
             *     "first_name": "Foo",
             *     "last_name": "Bar"
             *    }
             * }
             *
             * for upsert:
             * {
             *   "type": "upsert",
             *   "source": "PRONOTES",
             *   "key": "body",
             *   "value": "%envoy_id: 123%",
             *   "uuid": "7b832c4b-e92a-4521-9c11-575a42119348",
             *   "last_seen": "07/12/2015 12:30",
             *   "data": {
             *     "body": "envoy_id: 123"
             *   }
             * }
             *
             * Determine conflict with type = update || type = upsert:
             * - if the table have touchdate field then get the value
             * and compare it with last_seen value (datetime).
             * - if last_seen < touchdate then send an error message to
             * ecpr-cud queue with the same uuid:
             *
             * {
             *   "type": "error",
             *   "error": "conflict",
             *   "uuid": "7b832c4b-e92a-4521-9c11-575a42119348"
             * }
             *
             * - if the table do not have touchdate or last_seen >= touchdate
             * then update the record.
             *
             * - if type == 'upsert' and cannot update then insert.
             *
             */

            try
            {
                string msg_type = jsonData.type;
                string table_name = jsonData.source;
                string table_pkey = jsonData.key;
                string table_pval = jsonData.value;
                string table_last_seen = jsonData.last_seen;
                string table_uuid = jsonData.uuid;
                dynamic table_data = jsonData.data;

                if (!string.IsNullOrEmpty(msg_type) && !string.IsNullOrEmpty(table_name))
                {
                    table_name = table_name.ToUpper();

                    if (msg_type == "insert")
                    {
                        string qQuery = @"
                                    SET Context_Info 0x55555
                                    INSERT INTO {0} ({1}) VALUES ({2})
                                    ";
                        string pKeys;
                        string pVals;
                        this.buildINSERTINTO(table_data, out pKeys, out pVals);

                        qQuery = String.Format(qQuery, table_name, pKeys, pVals);
                        this.executeNonQuery(qQuery);
                    }
                    else
                    {
                        if (string.IsNullOrEmpty(table_pkey) || string.IsNullOrEmpty(table_pval))
                        {
                            syncError("Cannot UPDATE or DELETE because 'key' and 'value' not found or have empty value in the message", jsonData);
                        }
                        else
                        {
                            if (msg_type == "update" || msg_type == "upsert")
                            {
                                if (string.IsNullOrEmpty(table_last_seen))
                                    syncError("Cannot UPDATE because 'last_seen' not found or have empty value in the message", jsonData);
                                else
                                {
                                    if (!this.isValidDateTimeFormat(table_last_seen))
                                        syncError(String.Format("Cannot UPDATE because value of 'last_seen' is not correct datetime format, the correct format is: {0}", this.dateTimeFormat), jsonData);
                                    else
                                    {
                                        string qTQuery = "SELECT TOUCHDATE FROM {0} WHERE {1}";

                                        if (table_pval.StartsWith("%") || table_pval.EndsWith("%"))
                                            qTQuery += " LIKE ";
                                        else
                                            qTQuery += "=";

                                        if (this.isNumeric(table_pval))
                                            qTQuery += "{2}";
                                        else
                                            qTQuery += "'{2}'";

                                        using (SqlConnection _sqlConnection = new SqlConnection(this.config.cprDatabaseConnectionString))
                                        {
                                            SqlDataReader sqlDataReader = this.executeQuery(String.Format(qTQuery, table_name, table_pkey, table_pval), _sqlConnection);
                                            bool canUpdate = true;
                                            bool canInsert = false;

                                            if (sqlDataReader != null) // meant touchdate is exists in the table
                                            {
                                                if (sqlDataReader.HasRows) // meant where key=value is existing
                                                {
                                                    sqlDataReader.Read();

                                                    DateTime touchDate = sqlDataReader.GetDateTime(0);
                                                    DateTime lastSeen = DateTime.ParseExact(table_last_seen, this.dateTimeFormat, CultureInfo.InvariantCulture);

                                                    touchDate = DateTime.ParseExact(touchDate.ToString(this.dateTimeFormat), this.dateTimeFormat, CultureInfo.InvariantCulture);

                                                    if (lastSeen < touchDate) // we have conflict
                                                    {
                                                        syncError(String.Format("Cannot UPDATE because have conflict TouchDate({0}) - LastSeen({1}).", touchDate.ToString(), lastSeen.ToString()), jsonData);
                                                        Dictionary<string, dynamic> pMsg = new Dictionary<string, dynamic> {
                                                            {"type", "error"},
                                                            {"error", "conflict"},
                                                            {"uuid", table_uuid},
                                                            {"source", table_name},
                                                            {"key", table_pkey},
                                                            {"value", jsonData.value}
                                                        };
                                                        DataChangesCache.AddItem("DATA_CONFLICT:" + table_uuid,
                                                            JsonConvert.SerializeObject(pMsg, Formatting.Indented));
                                                        canUpdate = false;
                                                    }
                                                }
                                                else
                                                {
                                                    canUpdate = false;
                                                    if (msg_type == "upsert")
                                                        canInsert = true; // insert for upsert message
                                                }
                                                sqlDataReader.Close();
                                            }

                                            if (canUpdate)
                                            {
                                                string qQuery = @"
                                                SET Context_Info 0x55555
                                                UPDATE {0} SET {1} WHERE {2}";

                                                if (table_pval.StartsWith("%") ||
                                                     table_pval.EndsWith("%"))
                                                    qQuery += " LIKE ";
                                                else
                                                    qQuery += "=";

                                                if (this.isNumeric(table_pval))
                                                    qQuery += "{3};";
                                                else
                                                    qQuery += "'{3}';";

                                                qQuery = String.Format(qQuery, table_name, buildSET(table_data), table_pkey, table_pval);

                                                if (msg_type == "upsert")
                                                {
                                                    if (!this.executeNonQuery(qQuery))
                                                        canInsert = true;
                                                }
                                                else
                                                {
                                                    this.executeNonQuery(qQuery);
                                                }
                                            }
                                            else
                                            {
                                                if (msg_type == "update")
                                                    syncError(String.Format("Cannot UPDATE because the value {0} of the key {1} does not exists in db", table_pval, table_pkey), jsonData);
                                            }

                                            if (msg_type == "upsert")
                                            {
                                                if (canInsert)
                                                {
                                                    qTQuery = @"
                                                        SET Context_Info 0x55555
                                                        INSERT INTO {0} ({1}) VALUES ({2})";

                                                    string pKeys;
                                                    string pVals;
                                                    this.buildINSERTINTO(table_data, out pKeys, out pVals);
                                                    qTQuery = String.Format(qTQuery, table_name, pKeys, pVals);

                                                    this.executeNonQuery(qTQuery);
                                                }
                                            }

                                        }
                                    }
                                }
                            }
                            else if (msg_type == "delete")
                            {
                                string qQuery = @"
                                    SET Context_Info 0x55555
                                    DELETE FROM {0} WHERE {1}=";
                                if (this.isNumeric(table_pval))
                                    qQuery += "{2};";
                                else
                                    qQuery += "'{2}';";

                                this.executeNonQuery(String.Format(qQuery, table_name, table_pkey, table_pval));
                            }
                            else
                            {
                                syncError(String.Format("Unknown type == {0} on table {1}", msg_type, table_name), jsonData);
                            }
                        }
                    }

                }
                else
                {
                    syncError("Cannot process the json message from ebridge-cud without 'source' and 'type'", jsonData);
                }
            }
            catch (SqlException error)
            {
                if (error.Message.Contains("error: 19") ||
                    error.Message.Contains("error: 20") ||
                    error.Message.Contains("ExecuteReader") ||
                    error.Message.Contains("SHUTDOWN"))
                {
                    throw error;
                }
                else
                {
                    syncError(ECPRERROR.SqlExceptionDetail(error), jsonData);
                }
            }
            catch (Exception error)
            {
                if (error.Message.Contains("error: 19") ||
                    error.Message.Contains("error: 20") ||
                    error.Message.Contains("ExecuteReader") ||
                    error.Message.Contains("SHUTDOWN"))
                {
                    throw error;
                }
                else
                {
                    syncError(error.Message, jsonData);
                }
            }
        }

        private bool isValidDateTimeFormat(string dateTimeVal)
        {
            try
            {
                DateTime.ParseExact(dateTimeVal, this.dateTimeFormat, CultureInfo.InvariantCulture);
                return true;
            }
            catch (Exception)
            {
                return false;
            }
        }

        private bool executeNonQuery(string cText)
        {
            using (SqlConnection _sqlConnection = new SqlConnection(this.config.cprDatabaseConnectionString))
            {
                if (_sqlConnection.State != ConnectionState.Open)
                    _sqlConnection.Open();

                SqlTransaction trans;

                trans = _sqlConnection.BeginTransaction();

                using (SqlCommand sqlCommand = _sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = "SET NOCOUNT ON; " + cText;
                    sqlCommand.CommandType = CommandType.Text;
                    sqlCommand.CommandTimeout = this.sqlCommandTimeout;
                    sqlCommand.Transaction = trans;

                    int rowsAffected = sqlCommand.ExecuteNonQuery();
                    trans.Commit();

                    if (rowsAffected != 0)
                        return true;

                    return false;
                }
            }
        }

        private SqlDataReader executeQuery(string cText, SqlConnection _sqlConnection, CommandBehavior cBehavior = CommandBehavior.Default)
        {
            if (_sqlConnection.State != ConnectionState.Open)
                _sqlConnection.Open();

            SqlTransaction trans;

            trans = _sqlConnection.BeginTransaction(IsolationLevel.ReadUncommitted);

            using (SqlCommand sqlCommand = _sqlConnection.CreateCommand())
            {
                sqlCommand.CommandText = cText;
                sqlCommand.CommandType = CommandType.Text;
                sqlCommand.Notification = null;
                sqlCommand.CommandTimeout = this.sqlCommandTimeout;
                sqlCommand.Transaction = trans;

                return sqlCommand.ExecuteReader(cBehavior);
            }
        }

        private bool isNumeric(object e)
        {
            double retNum;
            bool isNum = Double.TryParse(Convert.ToString(e),
                System.Globalization.NumberStyles.Any,
                System.Globalization.NumberFormatInfo.InvariantInfo,
                out retNum);
            return isNum;
        }

        private string buildSET(dynamic table_data)
        {
            JObject jObject = table_data;
            Dictionary<string, dynamic> dData = jObject.ToObject<Dictionary<string, dynamic>>();

            List<string> kvP = new List<string>();
            string kvFormat;

            foreach (KeyValuePair<string, dynamic> dD in dData)
            {
                kvFormat = "{0}='{1}'";
                if (isNumeric(dD.Value))
                    kvFormat = "{0}={1}";
                kvP.Add(String.Format(kvFormat, dD.Key, dD.Value));
            }

            if (kvP.Count != 0)
                return string.Join(",", kvP.ToArray());

            return String.Empty;
        }

        private void buildINSERTINTO(dynamic table_data, out string pKeys, out string pVals)
        {
            pKeys = String.Empty;
            pVals = String.Empty;

            JObject jObject = table_data;
            Dictionary<string, dynamic> dData = jObject.ToObject<Dictionary<string, dynamic>>();

            List<string> pK = new List<string>();
            List<dynamic> pV = new List<dynamic>();

            foreach (KeyValuePair<string, dynamic> dD in dData)
            {
                pK.Add(dD.Key);

                if (isNumeric(dD.Value))
                    pV.Add(dD.Value);
                else
                    pV.Add(String.Format("'{0}'", dD.Value));
            }

            pKeys = string.Join(",", pK.ToArray());
            pVals = string.Join(",", pV.ToArray());
        }
    }
}
