using System;
using System.Linq;
using System.Threading;
using System.Diagnostics;
using System.Data.SqlClient;
using System.ServiceProcess;
using System.Security.Permissions;
using System.Data;
using System.Reflection;
using System.Collections.Generic;
using Newtonsoft.Json;
using System.Configuration;
using System.IO;

namespace CPREnvoy
{
    [System.ComponentModel.DesignerCategory("Code")]

    public partial class Envoy : ServiceBase
    {
        private string connectionString;
        private SqlConnection sqlConnection;
        private Logger logger;
        private Config config;

        private ManualResetEvent _stopEvent = new ManualResetEvent(false);
        private ManualResetEvent _inProcessEvent = new ManualResetEvent(false);
        private Thread _eventsThread;
        private System.Timers.Timer _eventsTimer;

        public bool isReadyToBeStarted = true;

        private SyncControlCenter syncControlCenter; // for CUD
        private FullSyncControlCenter fullsyncControlCenter; // for SYNC
        private ServiceControlCenter serviceControlCenter;
        private CPRDataAnalysis cprDataAnalysis;

        private int intervalSecs = 5000;

        public Envoy(Config config, Logger logger)
        {

            this.ServiceName = ConfigurationManager.AppSettings.Get("ecpr_service_name"); //"ECPR Service"
            this.connectionString = config.cprDatabaseConnectionString;

            this.logger = logger;
            this.config = config;

        }

        private SqlDataReader doSQL(string qStr, bool exeReader = true)
        {

            Utils.maybe_reconnect(this, ref sqlConnection, this.config.try_again_after, this.logger);

            using (SqlCommand sqlCommand = sqlConnection.CreateCommand())
            {
                sqlCommand.CommandText = qStr;
                sqlCommand.CommandType = CommandType.Text;
                sqlCommand.Notification = null;
                sqlCommand.CommandTimeout = this.config.defaultSQLQueryTimeout;

                try
                {
                    if (exeReader)
                    {
                        return sqlCommand.ExecuteReader();
                    }
                    else
                    {
                        sqlCommand.ExecuteNonQuery();
                        return null;
                    }
                }
                catch (Exception error)
                {
                    if (!error.Message.Contains("already an object"))
                    {
                        this.logger.write( ECPRERROR.ExceptionDetail(error), Logger.LOGLEVEL.ERROR);
                        this.isReadyToBeStarted = false;
                    }
                }
            }

            return null;
        }

        private bool ensureCPRDefaultSchema()
        {
            this.logger.write( "Ensure CPR Default Schema is DBO", Logger.LOGLEVEL.INFO);

            int isDBOOwner = 0;
            string queryStr = @"
            USE [{0}];
            SELECT is_rolemember('db_owner');
            ";

            SqlDataReader sqlDataReader = this.doSQL(String.Format(queryStr, this.config.cprDatabase));
            string schema_name = string.Empty;

            if (sqlDataReader != null)
            {
                while (sqlDataReader.Read())
                {
                    isDBOOwner = (int)sqlDataReader[sqlDataReader.GetName(0)];
                }
                sqlDataReader.Close();
            }

            if (isDBOOwner == 0)
            {
                this.logger.write( String.Format("ECPR is required user mapping default schema = [DBO] (db_owner role). Please verify and fix it ASAP!", schema_name.ToUpper()), Logger.LOGLEVEL.ERROR);
                this.logger.write( "Shutting down ECPR service ...", Logger.LOGLEVEL.INFO);
                return false;
            }

            return true;
        }

        private void ensureServiceBrokerIsEnabled()
        {
            if (this.config.cprDatabase != null)
            {
                var qStrVerifySeviceBroker = String.Format("SELECT is_broker_enabled FROM sys.databases WHERE name = '{0}'", this.config.cprDatabase);
                var qStrEnableServiceBroker = String.Format("ALTER DATABASE [{0}] SET ENABLE_BROKER WITH ROLLBACK IMMEDIATE", this.config.cprDatabase);
                var qStrCreateQueue = String.Format("CREATE QUEUE {0}", this.config.cprQueueName);
                var qStrCreateService = String.Format("CREATE SERVICE {0} ON QUEUE {1} ([http://schemas.microsoft.com/SQL/Notifications/PostQueryNotification])",
                    this.config.cprServiceName,
                    this.config.cprQueueName);
                var qStrGrantSubscribe = String.Format("GRANT SUBSCRIBE QUERY NOTIFICATIONS TO {0}", this.config.cprUsername);

                this.logger.write( String.Format("Verifying service broker is enabled on the database {0}", this.config.cprDatabase), Logger.LOGLEVEL.INFO);

                SqlDataReader sqlDataReader = this.doSQL(qStrVerifySeviceBroker);
                if (sqlDataReader != null)
                {
                    bool isEnabled = false;
                    while (sqlDataReader.Read())
                    {
                        isEnabled = (bool)sqlDataReader[sqlDataReader.GetName(0)];
                        if (isEnabled)
                            this.logger.write( "Service Broker is enabled", Logger.LOGLEVEL.INFO);
                    }
                    sqlDataReader.Close();

                    if (!isEnabled)
                    {
                        this.logger.write( String.Format("Service Broker is not enabled on the database {0}, try to enable it ...", this.config.cprDatabase), Logger.LOGLEVEL.INFO);

                        this.doSQL(qStrEnableServiceBroker, false);
                        if (!this.isReadyToBeStarted) return;

                        this.logger.write( "done!", Logger.LOGLEVEL.INFO);
                        this.logger.write( "Create queue ...", Logger.LOGLEVEL.INFO);
                        this.doSQL(qStrCreateQueue, false);
                        if (!this.isReadyToBeStarted) return;

                        this.logger.write( "done!", Logger.LOGLEVEL.INFO);
                        this.logger.write( "Create service on queue ...", Logger.LOGLEVEL.INFO);
                        this.doSQL(qStrCreateService, false);

                        this.logger.write( "done!", Logger.LOGLEVEL.INFO);
                        this.logger.write( "Grant subscribe query notifications", Logger.LOGLEVEL.INFO);
                        this.doSQL(qStrGrantSubscribe, false);

                        this.logger.write( "done!", Logger.LOGLEVEL.INFO);
                    }
                }
            }
        }

        private void ensureFieldsExisting()
        {
            if (this.config.cprTables != null)
            {
                bool hasError = false;
                foreach (string tableName in this.config.cprTables.Keys)
                {
                    string qStr = String.Format("select COLUMN_NAME from information_schema.columns where table_name = '{0}'", tableName);
                    using (SqlCommand sqlCommand = sqlConnection.CreateCommand())
                    {
                        sqlCommand.CommandText = qStr;
                        sqlCommand.CommandType = CommandType.Text;
                        sqlCommand.Notification = null;
                        sqlCommand.CommandTimeout = this.config.defaultSQLQueryTimeout;

                        using (SqlDataReader sqlDataReader = sqlCommand.ExecuteReader())
                        {
                            List<string> fields = new List<string>();
                            while (sqlDataReader.Read())
                            {
                                string fieldName = (string)sqlDataReader[sqlDataReader.GetName(0)];
                                fields.Add(fieldName);
                            }
                            sqlDataReader.Close();

                            foreach (string fieldName in this.config.cprTables[tableName])
                            {
                                if (!fields.Contains(fieldName))
                                {
                                    var error_msg = String.Format("The field name: {0} is not existing in the CPR+ table name: {1} on database.", fieldName, tableName);
                                    this.logger.write( error_msg);
                                    hasError = true;
                                }
                            }

                            fields = null;
                        }
                    }

                    if (hasError)
                    {
                        break;
                    }
                }

                if (hasError)
                {
                    isReadyToBeStarted = false;
                }
            }

        }

        internal void dumpTable(List<string> tableList, List<string> exclude_columns=null, string export_format="csv", string filterCond=null)
        {
            System.Threading.Tasks.Task.Factory.StartNew(() =>
            {

                foreach (string tableName in tableList)
                {

                    try
                    {

                        string fieldsName = "*";

                        if (this.config.dumpIgnoreFields != null && this.config.dumpIgnoreFields.ContainsKey(tableName.ToUpper()))
                        {
                            exclude_columns = new List<string>(this.config.dumpIgnoreFields[tableName.ToUpper()]);
                        }

                        if (exclude_columns != null && exclude_columns.Count > 0)
                        {
                            using (var command = new SqlCommand(String.Format("select lower(COLUMN_NAME) from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME = '{0}' and COLUMN_NAME not in ({1})", tableName, "'" + String.Join("','", exclude_columns.ToArray()) + "'"), this.sqlConnection))
                            {
                                command.CommandTimeout = this.config.defaultSQLQueryTimeout;

                                using (var reader = command.ExecuteReader())
                                {
                                    if (reader.HasRows)
                                    {
                                        exclude_columns.Clear();
                                        while (reader.Read())
                                        {
                                            exclude_columns.Add(reader.GetValue(0).ToString());
                                        }
                                        fieldsName = String.Join(",", exclude_columns.ToArray());
                                    }
                                }
                            }
                        }

                        this.logger.write( String.Format("Dumping table {0} to {1}", tableName, Path.GetTempPath()), Logger.LOGLEVEL.INFO);

                        using (SqlConnection _sqlConnection = new SqlConnection(this.config.cprDatabaseConnectionString))
                        {
                            try
                            {
                                if (_sqlConnection.State != ConnectionState.Open)
                                    _sqlConnection.Open();
                            }
                            catch (Exception) { }

                            if (export_format.ToLower() == "json")
                                TableDumper.DumpTableToFile_JSON(_sqlConnection, tableName, fieldsName, filterCond, String.Format("{0}{1}", this.config.cprCustomerCode, this.config.cprCustomerEnvironment), this.config.defaultSQLQueryTimeout);
                            else
                                TableDumper.DumpTableToFile(_sqlConnection, tableName, fieldsName, filterCond, String.Format("{0}{1}", this.config.cprCustomerCode, this.config.cprCustomerEnvironment), this.config.defaultSQLQueryTimeout);

                        }

                        this.logger.write( String.Format("Dumped table {0} into {1} file successfully and uploaded to s3.", tableName, export_format), Logger.LOGLEVEL.INFO);

                        string downloadLink = S3Uploader.GenerateDownloadLink(tableName, this.config.cprCustomerCode, this.config.cprCustomerEnvironment, export_format);

                        this.serviceControlCenter.publishMessage(JsonConvert.SerializeObject(new Dictionary<string, object>() {
                        {"customer", this.config.cprCustomerCode },
                        {"environment", this.config.cprCustomerEnvironment },
                        {"uuid", Guid.NewGuid().ToString() },
                        {"type", "url" },
                        {"table_name", tableName },
                        {"url", downloadLink }
                    }, new JsonSerializerSettings()
                    {
                        Formatting = Formatting.Indented,
                    }), "ecpr-config-s3-response", 2);

                    }
                    catch (S3UploadException error)
                    {
                        this.logger.write( String.Format("Cannot start uploading file {0}.{1}.gz to s3 because {2}.", tableName, export_format, error.ToString()), Logger.LOGLEVEL.ERROR);
                    }
                    catch (Exception error)
                    {
                        this.logger.write( String.Format("Cannot start dumping table {0} because {1}.", tableName, error.ToString()), Logger.LOGLEVEL.ERROR);
                        this.logger.write( ECPRERROR.ExceptionDetail(error), Logger.LOGLEVEL.ERROR);
                    }
                }
            });
        }

        internal void runQueryToFile(string runQuery, string resultFile, string resultType)
        {
            System.Threading.Tasks.Task.Factory.StartNew(() =>
            {

                string cusCodeEnv = String.Format("{0}{1}", this.config.cprCustomerCode, this.config.cprCustomerEnvironment);

                try
                {
                    Utils.maybe_reconnect(this, ref this.sqlConnection, this.config.try_again_after, this.logger);

                    TableDumper.runQueryToFile(this.sqlConnection, runQuery, resultFile, cusCodeEnv, this.config.defaultSQLQueryTimeout, resultType);

                    this.logger.write( String.Format("run_query {0} into {1} file successfully and uploaded to s3.", runQuery, resultFile), Logger.LOGLEVEL.INFO);
                }
                catch (S3UploadException error)
                {
                    this.logger.write( String.Format("Cannot start uploading file {0}.gz to s3 because {1}.", resultFile, error.ToString()), Logger.LOGLEVEL.ERROR);
                }
                catch (Exception error)
                {
                    this.logger.write( String.Format("Cannot start running run_query command: {0} because {1}.", runQuery, error.ToString()), Logger.LOGLEVEL.ERROR);
                    this.logger.write( ECPRERROR.ExceptionDetail(error), Logger.LOGLEVEL.ERROR);
                }
            });
        }

        internal void deleteTriggeraAndLogTables()
        {
            List<string> qQueries = new List<string> {
                @"SELECT CONCAT('DROP TRIGGER ', NAME,';' ) AS query
                                  FROM sys.triggers WHERE NAME LIKE  'ecpr_%'",
                @"SELECT CONCAT('DROP TABLE ',TABLE_SCHEMA,'.', TABLE_NAME,';' ) AS query
                                  FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE  'ecpr_%'"
            };

            List<string> fQueries = new List<string>();

            foreach(string qQuery in qQueries)
            {
                SqlDataReader sqlDataReader = doSQL(qQuery);

                if (sqlDataReader != null)
                {
                    while (sqlDataReader.Read())
                    {
                        fQueries.Add(sqlDataReader.GetString(0));
                    }
                    sqlDataReader.Close();
                }
            }

            doSQL(String.Join("", fQueries.ToArray()), false);
        }

        internal void stopConsumersPublishers()
        {
            this.syncControlCenter.Dispose();
            this.fullsyncControlCenter.Dispose();
            this.serviceControlCenter.Dispose();
        }

        private void ensureTablesExisting()
        {
            if (this.config.cprTables != null)
            {
                bool hasError = false;
                foreach (string tableName in this.config.cprTables.Keys)
                {
                    // Make sure this table is existing in database
                    string qStr = String.Format("select case when exists((select * from information_schema.tables where table_name = '{0}')) then 1 else 0 end", tableName);
                    using (SqlCommand sqlCommand = sqlConnection.CreateCommand())
                    {
                        sqlCommand.CommandText = qStr;
                        sqlCommand.CommandType = CommandType.Text;
                        sqlCommand.Notification = null;
                        sqlCommand.CommandTimeout = this.config.defaultSQLQueryTimeout;

                        using (SqlDataReader sqlDataReader = sqlCommand.ExecuteReader())
                        {
                            sqlDataReader.Read();
                            if ((int)sqlDataReader[0] == 0)
                            {
                                var error_msg = String.Format("The CPR+ table name: {0} is not existing in database.", tableName);
                                this.logger.write( error_msg);
                                hasError = true;
                            }
                            sqlDataReader.Close();

                            if (hasError)
                                break;
                        }
                    }
                }

                if (hasError)
                {
                    isReadyToBeStarted = false;
                }
            }
            else
            {
                this.logger.write( "There are no CPR+ tables.");
                isReadyToBeStarted = false;
            }
        }

        private void eventsProcess()
        {
            var waitHandles = new WaitHandle[] { this._stopEvent, this._inProcessEvent };

            while (!this._stopEvent.WaitOne(0))
            {
                var handle_code = WaitHandle.WaitAny(waitHandles);

                switch (handle_code)
                {
                    case 0:
                        this.logger.write( String.Format("Disconnecting to database {0}", this.config.cprDatabase), Logger.LOGLEVEL.INFO);

                        if (this.sqlConnection != null && (this.sqlConnection.State == ConnectionState.Open))
                            this.sqlConnection.Close();
                        break;
                    case 1:
                        if (syncControlCenter != null && serviceControlCenter != null)
                        {
                            if ((!config.radius_only_customer && !syncControlCenter.isSubscribed) ||
                                !serviceControlCenter.isSubscribed ||
                                (!config.radius_only_customer && !fullsyncControlCenter.isSubscribed) ||
                                !this.logger.isSubscribed)
                            {

                                this.logger.write( "Some of rabbitmq connections has been lost, re-connecting ...", Logger.LOGLEVEL.WARN);

                                if (!config.radius_only_customer && !syncControlCenter.isConnecting)
                                {
                                    syncControlCenter.reConnect();
                                    if (syncControlCenter.isSubscribed)
                                        syncControlCenter.consumerInit();
                                }

                                if (!serviceControlCenter.isConnecting)
                                {
                                    serviceControlCenter.reConnect();
                                    if (serviceControlCenter.isSubscribed)
                                        serviceControlCenter.consumerInit();
                                }

                                if (!config.radius_only_customer && !fullsyncControlCenter.isConnecting)
                                    fullsyncControlCenter.reConnect();

                                if (!this.logger.isConnecting)
                                    this.logger.ensureConnect();
                            }
                        }

                        foreach (string pKey in DataChangesCache.Keys())
                        {
                            if (!DataChangesCache.isPOSTing(pKey))
                            {
#if (DEBUG)
                                Console.WriteLine("SEND MESSAGE FOR KEY = {0}", pKey);
#endif
                                this.syncControlCenter.sendMessageByKey(pKey);
                            }
                        }

                        /*
                        var mem_using = (GC.GetTotalMemory(true) / 1024) / 1024;
                        if (mem_using > 1024) // Limit == 1024MB of memory
                        {
                            this.logger.write( "ECPR is consuming lot of memory, restart it!.", Logger.LOGLEVEL.FATAL);
                            Thread.Sleep(5000);
                            Environment.Exit(1);
                        }*/

                        GC.Collect();

                        this._inProcessEvent.Reset();
                        this._eventsTimer.Start();
                        break;
                }
            }
        }

        protected override void OnStart(string[] args)
        {
            base.OnStart(args);

            if (!config.is_Fetched_OK)
                return;

            Thread threadWorker = new Thread(new ThreadStart(
                delegate
                {
                    longPoll();
                }
             ));

            threadWorker.Start();
        }

        private void longPoll()
        {
            if (Utils.isStartingFromWindowsRestart())
                Thread.Sleep(this.config.wait_before_start_from_windows_restart * 1000);

            logger.write( "Checking internet/network connection...", Logger.LOGLEVEL.INFO);

            while (!NetworkMonitoring.IsInternetAvailable())
            {
                logger.write( String.Format("No internet connection. Will try again after {0} secs.", this.config.try_again_after), Logger.LOGLEVEL.ERROR);
                Thread.Sleep(this.config.try_again_after * 1000);
            }

            logger.write( "Internet/network connection is succesful.", Logger.LOGLEVEL.INFO);

            this.logger.ensureConnect();

            logger.write( "Checking SQL SERVER connection...", Logger.LOGLEVEL.INFO);

            string errorMessage;

            while (!Utils.try_connect_to_db(this.config.cprDatabaseConnectionString, out errorMessage))
            {
                logger.write( String.Format("Had an error while trying to connect to SQL SERVER. Will try again after {0} secs.", this.config.try_again_after), Logger.LOGLEVEL.ERROR);
                logger.write( errorMessage, Logger.LOGLEVEL.ERROR);
                Thread.Sleep(this.config.try_again_after * 1000);
            }

            logger.write( "SQL SERVER connection is succesful.", Logger.LOGLEVEL.INFO);

            logger.write( "STARTING ECPR service...", Logger.LOGLEVEL.INFO);

            this.serviceControlCenter = new ServiceControlCenter(this.config, this.logger);
            this.serviceControlCenter.addManager(this);

            this.logger.write( String.Format("Connecting to database {0} ...", this.config.cprDatabase), Logger.LOGLEVEL.INFO);

            this.sqlConnection = new SqlConnection(connectionString);
            Utils.maybe_reconnect(this, ref this.sqlConnection, this.config.try_again_after, this.logger);

            System.Timers.Timer ecprTimer = new System.Timers.Timer();
            ecprTimer.Elapsed += (sender, e) => Utils.maybe_reconnect(this, ref this.sqlConnection, this.config.try_again_after, this.logger);
            ecprTimer.Interval += (this.config.defaultDatabaseConnectionTimeout * 1000 + 5000);
            ecprTimer.Enabled = true;

            if (isReadyToBeStarted)
            {
                this.ensureTablesExisting();
                this.ensureFieldsExisting();
                this.getIgnoreFieldsPK();
            }

            if (sqlConnection.State == ConnectionState.Open)
            {
                this.syncControlCenter = new SyncControlCenter(this.config, this.logger);
                this.fullsyncControlCenter = new FullSyncControlCenter(this.config, this.logger);
            }


            if ((syncControlCenter != null && !syncControlCenter.isSubscribed) ||
                (serviceControlCenter != null && !serviceControlCenter.isSubscribed))
            {

                logger.write(
                    String.Format("Cannot subscribed to amqp channels, will try again in {0} secs !!!!!!!", intervalSecs / 1000),
                    Logger.LOGLEVEL.ERROR);
            }

            if (!this.config.needToFetchSchemaAndData)
            {
                dbSchemaPush();
                dbDataPush();
                this.config.needToFetchSchemaAndData = true;
            }

            if (sqlConnection.State == ConnectionState.Open)
            {
                this.isReadyToBeStarted = true;

                foreach (string tableName in this.config.cprTables.Keys)
                {
                    try
                    {
                        // Make sure table is exists before subscribe
                        bool tableExists = false;
                        SqlDataReader sqlDataReader = this.doSQL(String.Format("select TABLE_NAME from INFORMATION_SCHEMA.TABLES where TABLE_NAME ='{0}'", tableName));

                        if (sqlDataReader != null)
                        {
                            if (sqlDataReader.HasRows)
                            {
                                tableExists = true;
                            }
                            sqlDataReader.Close();
                        }

                        if (tableExists)
                        {
                            var qN = new QueryNotification(
                                    this.config.cprTables[tableName].ToArray(),
                                    tableName,
                                    this.logger,
                                    this.config,
                                    this.config.restListAPIJSONMapping,
                                    this.config.autoCleanupIfRestart);
                            qN.subscribe();
                        } else
                        {
                            this.logger.write( String.Format("Cannot subscribed the table {0} for query notification because it doesn't exists in database. Ignored it!", tableName), Logger.LOGLEVEL.ERROR);
                        }
                    }
                    catch (Exception error)
                    {
                        this.logger.write( ECPRERROR.ExceptionDetail(error), Logger.LOGLEVEL.ERROR);
                        this.isReadyToBeStarted = false;
                    }
                }

            }

            if (this.config.diff_tables != null && this.isReadyToBeStarted)
            {
                this.logger.write( "Enable DIFF Tables feature.", Logger.LOGLEVEL.INFO);
                cprDataAnalysis = new CPRDataAnalysis(this.config, this.logger);
                cprDataAnalysis.Init();
            }


            this._eventsThread = new Thread(eventsProcess);
            this._eventsThread.IsBackground = false;
            this._eventsThread.Start();

            this._eventsTimer = new System.Timers.Timer(this.intervalSecs);
            this._eventsTimer.AutoReset = false;
            this._eventsTimer.Elapsed += (sender, e) => this._inProcessEvent.Set();
            this._eventsTimer.Start();

            logger.write( "SERVICE STARTED!!!", Logger.LOGLEVEL.INFO);
        }

        private void getIgnoreFieldsPK()
        {
            if (this.config.ignore_fields == null)
                return;

            string sqlQuery = String.Format(@"
                SELECT OBJECT_NAME(ic.OBJECT_ID) as table_name,
                COL_NAME(ic.OBJECT_ID,ic.column_id) AS pk
                FROM    sys.indexes AS i INNER JOIN
                        sys.index_columns AS ic ON  i.OBJECT_ID = ic.OBJECT_ID
                                                AND i.index_id = ic.index_id
                WHERE   i.is_primary_key = 1 and OBJECT_NAME(ic.OBJECT_ID) in ({0})
            ", "'"+string.Join("','", this.config.ignore_fields.Keys.ToList())+"'");

            SqlDataReader sqlDataReader = this.doSQL(sqlQuery);

            if (sqlDataReader != null)
            {
                while (sqlDataReader.Read())
                {
                    string tableName = sqlDataReader.GetString(0);
                    string pkName = sqlDataReader.GetString(1);

                    if (this.config.ignore_fields[tableName]["pk"] ==  null)
                        this.config.ignore_fields[tableName].Add("pk", pkName);

                }
                sqlDataReader.Close();
            }

        }

        protected override void OnStop()
        {
            base.OnStop();
            DisposeALLs();
        }

        protected override void OnShutdown()
        {
            Utils.windowsRestarting();
            this.logger.write( "Windows is restarting!!!", Logger.LOGLEVEL.INFO);
            DisposeALLs();
            base.OnShutdown();
        }

        private void DisposeALLs()
        {
            this.isReadyToBeStarted = false;

            try
            {
                this.logger.Dispose();
                this.syncControlCenter.Dispose();
                this.fullsyncControlCenter.Dispose();
                this.serviceControlCenter.Dispose();
            }
            catch { }

            if (this._eventsTimer != null)
            {
                this._eventsTimer.Stop();
                this._eventsTimer.Dispose();
            }

            this._stopEvent.Set();

            if (this._eventsThread != null)
                this._eventsThread.Join();
        }

        public void dbDataPush()
        {
            foreach (string tableName in this.config.cprTableOrder)
            {
                dbDataPush(tableName);
            }
        }

        public void dbDataPush(string tableName, dynamic filter=null)
        {
            this.logger.write( String.Format("DATA PUSH :: {0}", tableName), Logger.LOGLEVEL.INFO);
            dbFullSync(tableName.ToUpper(), filter);
        }

        public void dbSchemaPush(string tableName=null)
        {
            if (sqlConnection.State == ConnectionState.Open)
            {
                QueryNotification fortheFistStart = new QueryNotification(
                                    null,
                                    null,
                                    this.logger,
                                    this.config,
                                    this.config.restListAPIJSONMapping, null);
                fortheFistStart.collect_current_schema(this.config.cprTables, this.config.cprTableOrder, tableName);
                fortheFistStart = null;
                GC.Collect();
            }
        }

        public void dbSchemaDump()
        {
            string dumpSchemaQuery = @"SELECT cl.table_name as 'TABLE_NAME',
                               cl.column_name as 'COLUMN_NAME',
                               t.constraint_type as 'CONSTRAINT_TYPE',
                               cl.DATA_TYPE as 'DATA_TYPE'
                            FROM INFORMATION_SCHEMA.COLUMNS cl
                            LEFT JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE cu
                             ON cl.table_catalog = cu.table_catalog
                               AND cl.table_schema = cu.table_schema
                               AND cl.table_name = cu.table_name
                               AND cl.column_name = cu.column_name
                            LEFT JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS t
                             ON cu.constraint_catalog = t.constraint_catalog
                               AND cu.constraint_schema = t.constraint_schema
                               AND cu.constraint_name = t.constraint_name";
            string csvFilename = "schema";
            string dumpResult = "failure";

            System.Threading.Tasks.Task.Factory.StartNew(() =>
            {
                try
                {

                    this.logger.write(String.Format(@"Dumping tables schema to {1}\{0}.csv", csvFilename, Path.GetTempPath()), Logger.LOGLEVEL.INFO);

                    TableDumper.QueryToFile(this.sqlConnection,
                        dumpSchemaQuery,
                        csvFilename,
                        String.Format("{0}{1}", this.config.cprCustomerCode, this.config.cprCustomerEnvironment),
                        this.config.defaultSQLQueryTimeout);

                    this.logger.write(String.Format("Dumped tables schema into {0} file successfully and uploaded to s3.", csvFilename), Logger.LOGLEVEL.INFO);

                    dumpResult = "success";
                }
                catch (S3UploadException error)
                {
                    this.logger.write(String.Format("Cannot start uploading file {0} to s3 because {1}.", csvFilename, error.ToString()), Logger.LOGLEVEL.ERROR);
                }
                catch (Exception error)
                {
                    this.logger.write(String.Format("Cannot start dumping tables schema because {0}.", error.ToString()), Logger.LOGLEVEL.ERROR);
                    this.logger.write(ECPRERROR.ExceptionDetail(error), Logger.LOGLEVEL.ERROR);
                }

                this.serviceControlCenter.publishMessage(JsonConvert.SerializeObject(new Dictionary<string, object>()
                {
                 {"customer", this.config.cprCustomerCode },
                 {"environment", this.config.cprCustomerEnvironment },
                 {"uuid", Guid.NewGuid().ToString() },
                 {"type", "dump_schema" },
                 {"result", dumpResult }
                }, new JsonSerializerSettings()
                {
                    Formatting = Formatting.Indented,
                }), "ecpr-config-s3-response", 2);

            });
        }

        public void dbFullSync(string tableName, dynamic filter=null)
        {
            if (sqlConnection.State == ConnectionState.Open)
            {
                QueryNotification fortheFistStart = new QueryNotification(
                                   this.config.cprTables[tableName].ToArray(),
                                   tableName,
                                   this.logger,
                                   this.config,
                                   this.config.restListAPIJSONMapping, filter);

                fortheFistStart.collect_current_data(this.fullsyncControlCenter);
                fortheFistStart = null;

                GC.Collect();
            }
        }

        public void countTable(string tableName, string filter)
        {
            var sqlQuery = String.Format("SELECT COUNT(*) FROM {0}", tableName);

            if (filter != String.Empty)
            {
                sqlQuery += " WHERE " + filter;
            }

            SqlDataReader sqlDataReader = this.doSQL(sqlQuery);

            if (sqlDataReader != null)
            {
                if (sqlDataReader.Read())
                {
                    var countRows = sqlDataReader.GetInt32(0);

                    this.syncControlCenter.publishMessage(JsonConvert.SerializeObject(new Dictionary<string, object>() {
                        {"customer", this.config.cprCustomerCode },
                        {"environment", this.config.cprCustomerEnvironment },
                        {"uuid", Guid.NewGuid().ToString() },
                        {"type", "count" },
                        {"table", tableName },
                        {"value", countRows }
                    }, new JsonSerializerSettings()
                    {
                        Formatting = Formatting.Indented,
                    }));
                }

                sqlDataReader.Close();
            }
        }

        public void dbTablesCleanUp()
        {
            /*
             * This function will be called when service config
             * is changed to removes temporary tables and trigger
             * if the table is not exists in new config but still
             * exists in old config.
             */
            if (this.sqlConnection.State != ConnectionState.Open)
                return;

            List<string> triggerTables = this.config.removeTriggerTables();
            if (triggerTables != null)
            {
                foreach (string tableName in triggerTables)
                {
                    string tableTempName = String.Format("ecpr_{0}_logs", tableName.ToLower());
                    string triggerName = String.Format("ecpr_{0}_data_change_trigger", tableName.ToLower());
                    this.logger.write( String.Format("Remove table temp {0} and trigger {1} of table {2}", tableTempName, triggerName, tableName), Logger.LOGLEVEL.INFO);

                    string strQuery = String.Format(@"
                        DROP TRIGGER {0};
                        DROP TABLE {1};
                    ", triggerName, tableTempName);

                    using (SqlCommand sqlCommand = sqlConnection.CreateCommand())
                    {
                        sqlCommand.CommandText = strQuery;
                        sqlCommand.CommandType = CommandType.Text;
                        sqlCommand.Notification = null;
                        sqlCommand.CommandTimeout = this.config.defaultSQLQueryTimeout;
                        sqlCommand.ExecuteNonQuery();
                    }
                }
            }
        }
    }
}
