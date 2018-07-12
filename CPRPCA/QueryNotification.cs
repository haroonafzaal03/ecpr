using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Security.Cryptography;
using System.Text;
using System.Data.Common;
using System.Runtime.Serialization.Formatters;
using System.Threading;
using Newtonsoft.Json.Converters;


namespace CPREnvoy
{
    static class DataTypeMapping
    {
        private static Dictionary<string, string> mapKeys = new Dictionary<string, string> {
            {"bigint,bit,init,smallint,tinyint", "interger"},
            {"numeric,decimal,money,smallmoney,float,real", "decimal"},
            {"datetime, datetime2, smalldatetime, datetimeoffset", "datetime"},
            {"date", "date"},
            {"time", "time"},
            {"timestamp", "timestamp"},
            {"char,varchar,text,nchar,nvarchar,ntext", "text"},
            {"binary,varbinary,image", "binary"}
        };

        public static string map(string pKey)
        {
            foreach (KeyValuePair<string, string> mapKey in mapKeys)
            {
                if (mapKey.Key.IndexOf(pKey, StringComparison.OrdinalIgnoreCase) >= 0)
                    return mapKey.Value;
            }
            return "unknown";
        }
    }

    class CPRRowData
    {
        public string type { get; set; }
        public string datetime { get; set; }
        public string source { get; set; }
        public string uuid { get; set; }
        public string customer { get; set; }
        public Dictionary<string, object> data { get; set; }

    }

    class CPRSchemaData
    {
        public string type = "schema";
        public string table { get; set; }
        public string datetime { get; set; }
        public string uuid { get; set; }
        public string customer { get; set; }
        public Dictionary<string, string> fields { get; set; }
    }

    public class CprPlusDateTimeConvertor : DateTimeConverterBase
    {
        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            return DateTime.Parse(reader.Value.ToString());
        }

        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            if (value == null)
            {
                writer.WriteValue("");
                return;
            }

            writer.WriteValue(Math.Abs(((DateTime) value).TimeOfDay.TotalSeconds) < 1
                ? ((DateTime) value).ToString("MM/dd/yyyy")
                : ((DateTime) value).ToString("MM/dd/yyyy hh:mm:ss tt"));
        }
    }

    public class QueryNotification
    {
        private string tableName;
        private string[] tableFieldsNotification;
        private List<int> ecpr_ids;
        public Dictionary<string, Dictionary<string, string>> json_mapping = null;
        private string sqlStrCommand;
        private Logger logger;
        private Config config;

        private dynamic tableFilter = null;
        private List<string>  tableFilterOperators = new List<string> { "=", "<>", ">", "<", ">=", "<=", "BETWEEN", "LIKE", "IN" };

        private JsonSerializerSettings jsonSerializerSettings = new JsonSerializerSettings()
        {
            //NullValueHandling = NullValueHandling.Ignore,
            Formatting = Formatting.Indented,
        };

        private bool autoCleanupIfRestart = true;

        public QueryNotification(string[] m_tableFields, string m_tableName,
            Logger logger,
            Config config,
            Dictionary<string, Dictionary<string, string>> json_mapping=null,
            dynamic filter=null)
        {

            jsonSerializerSettings.Converters.Add(new CprPlusDateTimeConvertor());

            this.tableFieldsNotification = m_tableFields;
            this.json_mapping = json_mapping;
            this.tableName = m_tableName;
            this.logger = logger;
            this.config = config;
            this.tableFilter = filter;

            if (this.tableFieldsNotification != null)
                this.reCheckTableFields();
        }

        public void collect_current_data(FullSyncControlCenter syncControlCenter)
        {
            string strQuery = String.Format("SELECT {0} FROM {1}", string.Join(",", this.tableFieldsNotification), this.tableName);
            List<SqlParameter> sqlParams = new List<SqlParameter>();

            if (this.tableFilter != null)
            {
                string _field = this.tableFilter["field"];
                string _op = this.tableFilter["op"];
                dynamic _value = this.tableFilter["value"];

                _field = _field.Trim();
                _op = _op.Trim();

                if (_field == String.Empty || _op == String.Empty || _value == null) {
                    this.logger.write( "Invalid sync.filter params.", Logger.LOGLEVEL.ERROR);
                    return;
                }
                else if (!this.tableFilterOperators.Contains(_op.ToUpper())) {
                    this.logger.write( String.Format("Invalid sync.filter.op operator. It should be in [{0}]",
                        String.Join(",", this.tableFilterOperators)),
                        Logger.LOGLEVEL.ERROR);
                    return;
                }
                else if (_op.ToUpper() == "IN" && !(_value is Newtonsoft.Json.Linq.JArray))
                {
                    this.logger.write( "Invalid sync.filter.value. It should be a list of values because sync.filter.op is 'IN'", Logger.LOGLEVEL.ERROR);
                    return;
                }
                else if(_op.ToUpper() != "IN" && _value is Newtonsoft.Json.Linq.JArray)
                {
                    this.logger.write( String.Format("Invalid sync.filter.value. It should be a value, NOT a list of values because sync.filter.op is '{0}'", _op.ToUpper()),
                        Logger.LOGLEVEL.ERROR);
                    return;
                }
                else
                {
                    if (_value is Newtonsoft.Json.Linq.JArray)
                    {
                        List<string> bindParams = new List<string>();
                        int i = 1;
                        foreach (Newtonsoft.Json.Linq.JValue jValue in _value)
                        {
                            var bindParam = String.Format("@FIELD_VALUE_{0}", i);
                            SqlParameter sqlParam = new SqlParameter();
                            sqlParam.ParameterName = bindParam;
                            sqlParam.Value = jValue.Value;
                            sqlParams.Add(sqlParam);
                            bindParams.Add(bindParam);
                            i++;
                        }

                        strQuery += String.Format(" WHERE {0} IN ({1})", _field, string.Join(",", bindParams));
                    } else
                    {
                        SqlParameter sqlParam = new SqlParameter();
                        sqlParam.ParameterName = "@FIELD_VALUE";

                        strQuery += String.Format(" WHERE {0} {1} @FIELD_VALUE", _field, _op);
                        sqlParam.Value = _value.Value;

                        sqlParams.Add(sqlParam);
                    }

                }
            }

            using (SqlConnection _sqlConnection = new SqlConnection(config.cprDatabaseConnectionString))
            {

                SqlDataReader sqlDataReader = this.executeQuery(strQuery, _sqlConnection, CommandBehavior.Default, sqlParams);

                if (sqlDataReader != null)
                {
                    if (sqlDataReader.HasRows)
                    {
                        while (sqlDataReader.Read())
                        {
                            var data = new Dictionary<string, object>();

                            for (int i = 0; i < sqlDataReader.FieldCount; i++)
                            {
                                //if (!sqlDataReader.IsDBNull(i))
                                try
                                {
                                    data[sqlDataReader.GetName(i).ToLower()] = sqlDataReader[i];
                                }
                                catch (InvalidOperationException)
                                {
                                    //FIXME: Transaction lose!!!
                                    //Need to push this message to data cache
                                    //To get it push to rabbitmq in the next
                                    //retry. *sigh*
                                }
                            }

                            var cprRowData = new CPRRowData()
                            {
                                type = "fullsync",
                                datetime = Utils.getUnixTimeStamp(),
                                source = this.tableName,
                                uuid = Guid.NewGuid().ToString(),
                                data = data,
                                customer = this.config.cprCustomerCode
                            };

                            syncControlCenter.publishMessage(JsonConvert.SerializeObject(cprRowData, this.jsonSerializerSettings));
                        }
                        this.logger.write(String.Format("Sent COLLECTALLDATA_{0} to amqp broker server.", this.tableName), Logger.LOGLEVEL.INFO);
                    }

                    sqlDataReader.Close();
                }

            }
        }

        public void collect_current_schema(Dictionary<string, List<string>> cprTables,
                List<string> cprTableOrder, string tableEqualName=null)
        {
            List<CPRSchemaData> schemaList = new List<CPRSchemaData>();
            List<string> pKeys = new List<string>(cprTables.Keys);

            foreach (string tableName in cprTableOrder)
            {
                if (tableEqualName != null && tableName.ToLower() != tableEqualName.ToLower())
                    continue;

                if (!pKeys.Contains(tableName))
                {
                    this.logger.write( String.Format("Cannot fetch schema because table order name {0} does not exists in tables list of ecpr config.", tableName), Logger.LOGLEVEL.ERROR);
                    continue;
                }

                string inFields;

                if (cprTables[tableName].Count > 0)
                    inFields = string.Join(",", cprTables[tableName].ToArray()).ToLower();
                else
                    inFields = string.Join(",", this.reCheckTableFields(tableName)).ToLower();

                string strQuery = String.Format("SELECT column_name,data_type FROM information_schema.columns WHERE table_name = '{0}'", tableName);

                using (SqlConnection _sqlConnection = new SqlConnection(config.cprDatabaseConnectionString))
                {
                    SqlDataReader sqlDataReader = this.executeQuery(strQuery, _sqlConnection);

                    if (sqlDataReader != null)
                    {
                        if (sqlDataReader.HasRows)
                        {

                            Dictionary<string, string> schemaFields = new Dictionary<string, string>();

                            foreach (DbDataRecord dataRecord in sqlDataReader)
                            {
                                string columnName = dataRecord.GetString(0).ToLower();
                                if (inFields.Contains(columnName))
                                {
                                    string dataType = DataTypeMapping.map(dataRecord.GetString(1));
                                    schemaFields.Add(columnName, dataType);
                                }
                            }

                            schemaList.Add(new CPRSchemaData
                            {
                                customer = this.config.cprCustomerCode,
                                fields = schemaFields,
                                datetime = Utils.getUnixTimeStamp(),
                                table = tableName,
                                uuid = Guid.NewGuid().ToString()
                            });
                        }

                        sqlDataReader.Close();
                    }
                }
            }

            if (schemaList.Count > 0)
            {
                if (schemaList.Count == 1)
                    DataChangesCache.AddItem(Guid.NewGuid().ToString(), JsonConvert.SerializeObject(schemaList[0], Formatting.Indented));
                else
                    DataChangesCache.AddItem("COLLECTTABLESCHEMAS", JsonConvert.SerializeObject(schemaList, Formatting.Indented));

                this.logger.write( String.Format("SCHEMA PUSH :: {0}", tableName), Logger.LOGLEVEL.INFO);
            } else
            {
                if (tableEqualName != null)
                    this.logger.write( String.Format("Cannot fetch schema because table order name '{0}' does not exists in tables list of ecpr config.", tableEqualName), Logger.LOGLEVEL.ERROR);
            }

            schemaList = null;
            pKeys = null;
        }

        public QueryNotification(
            string[] m_tableFields,
            string m_tableName,
            Logger logger,
            Config config,
            Dictionary<string, Dictionary<string, string>> json_mapping=null,
            bool m_autoCleanupIfRestart=false)
        {
            jsonSerializerSettings.Converters.Add(new CprPlusDateTimeConvertor());

            this.tableName = m_tableName;
            this.tableFieldsNotification = m_tableFields;
            this.autoCleanupIfRestart = m_autoCleanupIfRestart;
            this.logger = logger;
            this.config = config;
            this.ecpr_ids = new List<int>();
            this.json_mapping = json_mapping;

            this.reCheckTableFields();

            buildStrCommand();
            ensureCloneTable();

            try
            {
                ensureEventsTriggerIsCreated();
            }catch(Exception error)
            {
                this.logger.write( String.Format("Cannot create or update trigger for table {0} because {1}", this.tableName, error.Message), Logger.LOGLEVEL.ERROR);
                this.logger.write( ECPRERROR.ExceptionDetail(error), Logger.LOGLEVEL.ERROR);
            }

            if (this.autoCleanupIfRestart)
            {
                var info_msg = String.Format("Clear data in the table ecpr_{0}_logs", this.tableName);
                this.logger.write( info_msg, Logger.LOGLEVEL.INFO);

                using (SqlConnection _sqlConnection = new SqlConnection(config.cprDatabaseConnectionString))
                {
                    this.executeNonQuery(String.Format("DELETE FROM ecpr_{0}_logs", this.tableName), _sqlConnection);
                }
            }

            this.logger.write( String.Format("Subscribed on table {0}", this.tableName), Logger.LOGLEVEL.INFO);
        }

        private void reCheckTableFields()
        {
            /* If there are no fields specified in a table then
             * get all of them from sql server cpr table.
             *
             * e.g.:
             *
             * tables['foo'] == []
             */
            if (this.tableFieldsNotification.Length == 0)
            {
                using (SqlConnection _sqlConnection = new SqlConnection(config.cprDatabaseConnectionString))
                {
                    SqlDataReader sqlDataReader = this.executeQuery(String.Format("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='{0}'", this.tableName), _sqlConnection);
                    if (sqlDataReader.HasRows)
                    {
                        var newList = new List<string>();
                        while (sqlDataReader.Read())
                        {
                            newList.Add(sqlDataReader["COLUMN_NAME"].ToString());
                        }
                        this.tableFieldsNotification = newList.ToArray();
                    }
                    sqlDataReader.Close();
                }
            }

#if (DEBUG)
            Console.WriteLine("Table {0} Fields Notification : {1}", this.tableName, string.Join(",", this.tableFieldsNotification));
#endif
        }

        private string[] reCheckTableFields(string _tableName)
        {
            using (SqlConnection _sqlConnection = new SqlConnection(config.cprDatabaseConnectionString))
            {
                SqlDataReader sqlDataReader = this.executeQuery(String.Format("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='{0}'", _tableName), _sqlConnection);
                var newList = new List<string>();

                if (sqlDataReader.HasRows)
                {
                    while (sqlDataReader.Read())
                    {
                        newList.Add(sqlDataReader["COLUMN_NAME"].ToString());
                    }
                }
                sqlDataReader.Close();

                return newList.ToArray();
            }
        }

        private void ensureCloneTable()
        {
            var info_msg = String.Format("Creating log table ecpr_{0}_logs ...", this.tableName);
            this.logger.write( info_msg, Logger.LOGLEVEL.INFO);

            string strQuery = String.Format("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'ecpr_{0}_logs'", this.tableName);

            using (SqlConnection _sqlConnection = new SqlConnection(config.cprDatabaseConnectionString))
            {
                var result = Convert.ToInt32(this.executeQueryScalar(strQuery, _sqlConnection));
                if (result > 0)
                {
                    this.logger.write(String.Format("Table ecpr_{0}_logs already created!", this.tableName), Logger.LOGLEVEL.INFO);
                    return;
                }
            }

            // Clone the table to ecpr_<table name>_logs
            strQuery = @"
                SELECT TOP 0 {1} INTO ecpr_{0}_logs FROM {0}
                UNION ALL
                SELECT TOP (1) {1} FROM {0} WHERE 1 = 2
                DELETE FROM ecpr_{0}_logs";

            using (SqlConnection _sqlConnection = new SqlConnection(config.cprDatabaseConnectionString))
            {
                if (!this.executeNonQuery(String.Format(strQuery, this.tableName, string.Join(",", this.tableFieldsNotification)), _sqlConnection))
                {
                    throw new Exception(String.Format("Cannot create log table ecpr_{0}_logs ...", this.tableName));
                }
            }
            // Add 'ecpr_action' and 'ecpr_action' fields to ecpr_<table name>_logs
            strQuery = @"
                ALTER TABLE ecpr_{0}_logs
                ADD ecpr_id INT IDENTITY;
                ALTER TABLE ecpr_{0}_logs
                ADD CONSTRAINT PK_ecpr_{0}_logs
                PRIMARY KEY(ecpr_id);
                ALTER TABLE ecpr_{0}_logs
                ADD ecpr_action VARCHAR(60) NOT NULL DEFAULT '';
                ALTER TABLE ecpr_{0}_logs
                ADD ecpr_datetime DATETIME NOT NULL DEFAULT GETUTCDATE();
                ALTER TABLE ecpr_{0}_logs
                ADD ecpr_status INT NOT NULL DEFAULT 0;
                ";

            using (SqlConnection _sqlConnection = new SqlConnection(config.cprDatabaseConnectionString))
            {
                if (!this.executeNonQuery(String.Format(strQuery, this.tableName), _sqlConnection))
                {
                    throw new Exception(String.Format("Cannot add ecpr meta field into log table ecpr_{0}_logs ...", this.tableName));
                }
            }

            this.logger.write( "done!", Logger.LOGLEVEL.INFO);
        }

        private void ensureEventsTriggerIsCreated()
        {
            var info_msg = String.Format("Creating trigger for table {0} ...", this.tableName);
            this.logger.write( info_msg, Logger.LOGLEVEL.INFO);

            string ignore_fields_script = "";
            string trigger_action = "CREATE";

            if (this.config.ignore_fields != null)
            {
                List<string> ignore_fields_tables = new List<string>(this.config.ignore_fields.Keys);
                if (ignore_fields_tables.Contains(this.tableName))
                {
                    this.logger.write( String.Format("Setting up ignore fields for table {0}", this.tableName), Logger.LOGLEVEL.INFO);

                    List<string> check_fields = new List<string>();
                    string ingore_fs = string.Join(",", this.config.ignore_fields[this.tableName]["fields"]).ToLower();

                    foreach(string f in this.tableFieldsNotification)
                    {
                        if (!ingore_fs.Contains(f.ToLower()))
                            check_fields.Add(String.Format("((table_1.{0} != table_2.{0}) OR (table_1.{0} IS NOT NULL AND table_2.{0} IS NULL) OR (table_1.{0} IS NULL AND table_2.{0} IS NOT NULL))", f));
                    }

                    ignore_fields_script = String.Format(@"
          IF @ACTION = 'update'
          BEGIN
                IF NOT EXISTS(
                  SELECT * FROM [dbo].[{0}] AS table_1 INNER JOIN deleted table_2 ON table_2.{1} = table_1.{1} WHERE
                  {2}
                )
                SET @ACTION = 'touch'
          END
                        ", this.tableName,
                        this.config.ignore_fields[this.tableName]["pk"],
                        string.Join(" OR ", check_fields),
                        "'" + string.Join("','", this.config.ignore_fields[this.tableName]["fields"]) + "'");
                }
            }

            string strQuery = String.Format("SELECT COUNT(*) FROM sys.triggers WHERE name = 'ecpr_{0}_data_change_trigger'", this.tableName);

            using (SqlConnection _sqlConnection = new SqlConnection(config.cprDatabaseConnectionString))
            {
                var result = Convert.ToInt32(this.executeQueryScalar(strQuery, _sqlConnection));
                if (result > 0)
                {
                    trigger_action = "ALTER";
                }
            }

            strQuery = @"
                {3} TRIGGER [dbo].[ecpr_{0}_data_change_trigger]
                ON [dbo].[{0}]
                AFTER INSERT, UPDATE, DELETE
                AS
                BEGIN
                    DECLARE @Context_Info VARBINARY(128)
                    SELECT @Context_Info = Context_Info()
                    IF @Context_Info = 0x55555
                        RETURN

                  DECLARE @ACTION nvarchar(30)

                  SET @ACTION = (CASE WHEN EXISTS(SELECT {1} FROM INSERTED)
                                         AND EXISTS(SELECT {1} FROM DELETED)
                                        THEN 'update'
                                        WHEN EXISTS(SELECT {1} FROM INSERTED)
                                        THEN 'insert'
                                        ELSE 'delete'
                                    END)

                    {2}

                  IF @ACTION = 'update' OR @ACTION = 'insert' OR @ACTION = 'touch'
                      BEGIN
                        INSERT [dbo].[ecpr_{0}_logs]
                        ({1},ECPR_ACTION)
                        (SELECT {1},@ACTION FROM inserted)
                      END
                    ELSE IF @ACTION = 'delete'
                      BEGIN
                        INSERT [dbo].[ecpr_{0}_logs]
                        ({1},ECPR_ACTION)
                        (SELECT {1},@ACTION FROM deleted)
                      END
                END";

            using (SqlConnection _sqlConnection = new SqlConnection(config.cprDatabaseConnectionString))
            {
                executeNonQuery(String.Format(strQuery, this.tableName, string.Join(",", this.tableFieldsNotification), ignore_fields_script, trigger_action), _sqlConnection);
            }
        }

        private bool executeNonQuery(string cText, SqlConnection _sqlConnection)
        {
            try
            {
                if (_sqlConnection.State != ConnectionState.Open)
                    _sqlConnection.Open();
            }
            catch (Exception) {
                return false;
            }

            SqlTransaction trans;

            trans = _sqlConnection.BeginTransaction();

            using (SqlCommand sqlCommand = _sqlConnection.CreateCommand())
            {
                sqlCommand.CommandText = cText;
                sqlCommand.CommandType = CommandType.Text;
                sqlCommand.Notification = null;
                sqlCommand.CommandTimeout = this.config.defaultSQLQueryTimeout;
                sqlCommand.Transaction = trans;

                try
                {
                    sqlCommand.ExecuteNonQuery();
                    trans.Commit();
                    return true;
                }
                catch (System.InvalidOperationException)
                {
                    return false;
                }
                catch (SqlException error)
                {
                    try
                    {
                        trans.Rollback();
                    }
                    catch (SqlException)
                    {
                    }

                    if (error.Message.Contains("error: 19") ||
                        error.Message.Contains("error: 20") ||
                        error.Message.Contains("SHUTDOWN"))
                    {
                        /* A transport-level error has occurred when sending the request to the server.
                         * (provider: Session Provider, error: 19 - Physical connection is not usable) */
                    } else
                    if (!error.Message.Contains("already an object"))
                    {
                        this.logger.write( ECPRERROR.SqlExceptionDetail(error), Logger.LOGLEVEL.ERROR);
                        return false;
                    }
                    return true;
                }
            }
        }

        private SqlDataReader executeQuery(string cText, SqlConnection _sqlConnection, CommandBehavior cBehavior = CommandBehavior.Default, List<SqlParameter> sqlParams = null)
        {
            try
            {
                if (_sqlConnection.State != ConnectionState.Open)
                    _sqlConnection.Open();
            }
            catch (Exception)
            {
                return null;
            }

            SqlTransaction trans;

            trans = _sqlConnection.BeginTransaction(IsolationLevel.ReadUncommitted);

            using (SqlCommand sqlCommand = _sqlConnection.CreateCommand())
            {
                sqlCommand.CommandText = cText;
                sqlCommand.CommandType = CommandType.Text;
                sqlCommand.Notification = null;
                sqlCommand.CommandTimeout = this.config.defaultSQLQueryTimeout;
                sqlCommand.Transaction = trans;

                if (sqlParams != null) {
                    foreach(SqlParameter sqlParam in sqlParams)
                    {
                        sqlCommand.Parameters.Add(sqlParam);
                    }
                }


                try
                {
                    return sqlCommand.ExecuteReader(cBehavior);
                }
                catch (System.InvalidOperationException)
                {
                    return null;
                }
                catch (SqlException error)
                {
                    if (error.Message.Contains("error: 19") ||
                        error.Message.Contains("error: 20") ||
                        error.Message.Contains("SHUTDOWN"))
                    {
                        /* A transport-level error has occurred when sending the request to the server.
                         * (provider: Session Provider, error: 19 - Physical connection is not usable) */
                    }
                    else
                        this.logger.write( ECPRERROR.SqlExceptionDetail(error), Logger.LOGLEVEL.ERROR);
                }

            }

            return null;
        }

        private object executeQueryScalar(string cText, SqlConnection _sqlConnection)
        {
            try
            {
                if (_sqlConnection.State != ConnectionState.Open)
                    _sqlConnection.Open();
            }
            catch (Exception)
            {
                return null;
            }

            SqlTransaction trans;

            trans = _sqlConnection.BeginTransaction(IsolationLevel.ReadUncommitted);

            using (SqlCommand sqlCommand = _sqlConnection.CreateCommand())
            {
                sqlCommand.CommandText = cText;
                sqlCommand.CommandType = CommandType.Text;
                sqlCommand.Notification = null;
                sqlCommand.CommandTimeout = this.config.defaultSQLQueryTimeout;
                sqlCommand.Transaction = trans;

                try
                {
                    return sqlCommand.ExecuteScalar();
                }
                catch (System.InvalidOperationException)
                {
                    return null;
                }
                catch (SqlException error)
                {
                    if (error.Message.Contains("error: 19") ||
                        error.Message.Contains("error: 20") ||
                        error.Message.Contains("SHUTDOWN"))
                    {
                        /* A transport-level error has occurred when sending the request to the server.
                         * (provider: Session Provider, error: 19 - Physical connection is not usable) */
                    } else
                        this.logger.write( ECPRERROR.SqlExceptionDetail(error), Logger.LOGLEVEL.ERROR);
                }
            }

            return null;
        }

        private IEnumerable<CPRRowData> SerializeCPR(SqlDataReader reader)
        {
            var results = new List<CPRRowData>();
            var cols = new List<string>();
            var recordType = "fullsync";

            for (var i = 0; i < reader.FieldCount; i++)
                if (!reader.GetName(i).StartsWith("ecpr_"))
                    cols.Add(reader.GetName(i));

            while (reader.Read())
            {
                try
                {
                    recordType = reader["ecpr_action"].ToString();
                    this.ecpr_ids.Add((int)reader["ecpr_id"]);
                }
                catch (Exception) {
                }

                Dictionary<string, object> origin_data = this.SerializeRowCPR(cols, reader);

                if (recordType ==  "touch")
                    if(this.config.ignore_fields[this.tableName]["send_touch"] != null &&
                        (bool)this.config.ignore_fields[this.tableName]["send_touch"] == true)
                    {
                        string pk_q = this.config.ignore_fields[this.tableName]["pk"].ToString().ToLower();

                        origin_data = new Dictionary<string, object>() {
                        {"touchdate", origin_data["touchdate"]},
                        {"pk", pk_q},
                        {"pk_value", origin_data[pk_q] }
                        };
                    } else
                    {
                        origin_data = null;
                    }

                if (origin_data != null)
                {
                    results.Add(new CPRRowData()
                    {
                        type = recordType,
                        datetime = Utils.getUnixTimeStamp(),
                        source = this.tableName,
                        uuid = Guid.NewGuid().ToString(),
                        data = origin_data,
                        customer = reader["ecpr_id"].ToString() // This is a quick hack to get ecpr_id
                    });
                }

            }

            return results;
        }

        private Dictionary<string, object> SerializeRowCPR(IEnumerable<string> cols, SqlDataReader reader)
        {
            var result = new Dictionary<string, object>();

            foreach (var col in cols)
                //if (reader[col] != DBNull.Value)
                {
                    object readerVal = reader[col];
                    //if (readerVal.ToString() == String.Empty)
                        //continue;
                    if (this.json_mapping != null
                        && this.json_mapping.ContainsKey(this.tableName)
                        && this.json_mapping[this.tableName].ContainsKey(col))
                    {
                        result.Add(this.json_mapping[this.tableName][col].ToLower(), readerVal);
                    }
                    else
                        result.Add(col.ToLower(), readerVal);
                }

            return result;
        }

        private byte[] GetHash(string inputString)
        {
            HashAlgorithm algorithm = MD5.Create();  //or use SHA1.Create();
            return algorithm.ComputeHash(Encoding.UTF8.GetBytes(inputString));
        }

        private string GetHashString(string inputString)
        {
            StringBuilder sb = new StringBuilder();
            foreach (byte b in GetHash(inputString))
                sb.Append(b.ToString("X2"));

            return sb.ToString();
        }

        private void executeQueryLogs(string cText)
        {
            using (SqlConnection _sqlConnection = new SqlConnection(config.cprDatabaseConnectionString))
            {
                SqlDataReader sqlDataReader = this.executeQuery(cText, _sqlConnection);

                if (sqlDataReader != null)
                {

                    if (sqlDataReader.HasRows)
                    {
                        foreach (CPRRowData cprRowData in this.SerializeCPR(sqlDataReader))
                        {
                            string ecpr_id = cprRowData.customer;
                            cprRowData.customer = this.config.cprCustomerCode;

                            string jsonConvert = JsonConvert.SerializeObject(cprRowData, this.jsonSerializerSettings);
                            //DataChangesCache.AddItem(GetHashString(jsonConvert), jsonConvert);
                            DataChangesCache.AddItem(String.Format("ECPR_CUD_ID:{0}:{1}", this.tableName, ecpr_id), jsonConvert);

                            if (cprRowData.type == "update" && this.config.diff_tables != null)
                            {
                                System.Threading.Tasks.Task.Factory.StartNew(() =>
                                {
                                    CPRDataAnalysis.compareDataDiff(this.tableName, cprRowData.data, cprRowData.datetime);
                                });
                            }
                        };
                    }

                    sqlDataReader.Close();
                }
            }
        }

        private void buildStrCommand()
        {
            if (this.tableFieldsNotification.Length != 0)
            {
                this.sqlStrCommand = String.Format("SELECT {0} FROM [dbo].[{1}]", string.Join(",", this.tableFieldsNotification), this.tableName);
            }
        }

        private void onDataTableChange(object source, System.Timers.ElapsedEventArgs e)
        {
            if (Utils.isReconnecting)
                return;

            if (!haveLogRecords())
            {
                return;
            }

            using (SqlConnection _sqlConnection = new SqlConnection(config.cprDatabaseConnectionString))
            {
                this.executeNonQuery(String.Format(@"
                declare @TotalCount int
                declare @TotalStatus1 int

                set @TotalCount = (select count(*) from ecpr_{0}_logs);
                set @TotalStatus1 = (select count(*) from ecpr_{0}_logs where ecpr_status=1);

                if @TotalCount = @TotalStatus1
                begin
                  update ecpr_{0}_logs set ecpr_status=0;
                end
                ", tableName), _sqlConnection);


                this.executeNonQuery(String.Format("DELETE FROM [dbo].[ecpr_{0}_logs] WHERE ecpr_status=2", this.tableName), _sqlConnection);
            }

            this.executeQueryLogs(String.Format("SELECT TOP 10 {0},ecpr_action,ecpr_datetime,ecpr_id FROM [dbo].[ecpr_{1}_logs] WHERE ecpr_status=0 ORDER BY ecpr_datetime", string.Join(",", this.tableFieldsNotification), this.tableName));

            if (this.ecpr_ids != null && this.ecpr_ids.Count > 0)
            {

                /* There are lot of issues comes over rabbitmq connections,
                 * just set all rows to ecpr_status = 1 before send CUD messages
                 * instead of delete them.
                 *
                 * if one or more messages failed to send then set ecpr_status back
                 * to 0 means ecpr_status = 0.
                 *
                 * if one ore more message sent succesfuly then set ecpr_status = 2.
                 * and ecpr will be deleted them later.
                 * */

                using (SqlConnection _sqlConnection = new SqlConnection(config.cprDatabaseConnectionString))
                {
                    // if row action == 'touch' and send_touch = false then just delete them
                    if (this.config.ignore_fields[this.tableName]["send_touch"] == null || (this.config.ignore_fields[this.tableName]["send_touch"] != null &&
                    (bool)this.config.ignore_fields[this.tableName]["send_touch"] == false))
                    {
                        this.executeNonQuery(String.Format("DELETE FROM [dbo].[ecpr_{0}_logs] WHERE ecpr_id IN ({1}) AND ecpr_action='touch'", this.tableName, string.Join(",", this.ecpr_ids)), _sqlConnection);
                    }
                    else
                        // else set ecpr_status = 1
                        this.executeNonQuery(String.Format("UPDATE [dbo].[ecpr_{0}_logs] SET ecpr_status=1 WHERE ecpr_id IN ({1})", this.tableName, string.Join(",", this.ecpr_ids)), _sqlConnection);
                }
            }
            this.ecpr_ids.Clear();
        }

        private bool haveLogRecords()
        {
            using (SqlConnection _sqlConnection = new SqlConnection(config.cprDatabaseConnectionString))
            {
                int count = Convert.ToInt32(this.executeQueryScalar(String.Format("SELECT COUNT(*) FROM ecpr_{0}_logs", this.tableName), _sqlConnection));
                return count > 0;
            }
        }

        public void subscribe()
        {
            System.Timers.Timer ecprTimer = new System.Timers.Timer();
            ecprTimer.Elapsed += new System.Timers.ElapsedEventHandler(onDataTableChange);
            ecprTimer.Interval += 10000;
            ecprTimer.Enabled = true;
        }

    }
}
