using System;
using System.Collections.Generic;
using System.Text;
using Newtonsoft.Json;
using System.Threading;
using System.Diagnostics;
using System.Configuration;
using System.ServiceProcess;


/*
 * List of available commands:
 *
 * {"config": "https://example.com/api/config/ecpr.json"} HB-1076
 * {"admin": "fullsync"} HB-1079
 * {"admin": "restart"}
 * {"admin": "reload"} //removes triggers/log tables and restart service.
 * {"sync": "table_name"}
 * {"sync": "table_name", "filter": {"field": "foo", "op": "=", "value": 1}}
 * {"sync": "table_name", "filter": {"field": "foo", "op": "in", "value": [1,2,3]}}
 * {"admin": "timezone"}
 *
 * {"schema": "table_name"}
 * returns latest table schema in a dict.
 * e.g.:
 *
 * {"type": "schema",
 * "table": "HR",
 * "datetime": "12345678",
 * "uuid": "xyz",
 * "customer": "ccc",
 * "fields": {
 *    ...
 *    "siteno": "interger",
 *    "sitename": "text",
 *    ...
 *    }
 * }
 * 
 * {"admin": "schema"} HB-1077
 * returns all latest tables schema in a list of dict.
 * 
 * {"admin": "dump_schema"}
 * dump tables schema into a csv file and upload to s3.
 * 
 * {"admin": "ping"}
 * need to add new queue name is 'ecpr-config-response' bind with exchange name 'control'
 * need to grant write permission for ecpr user to vhost /control .*
 *
 * {"admin": "update", "version": "3.30.0"}
 *
 * dump a single table:
 * {"dump":"table_name", "format": "json"}
 * {"dump":"table_name", "exclude_columns": ["foo", "bar"]}
 *
 * dump a single table with filter condition:
 * {"dump":"table_name", "filter":"foo='bar'"}
 *
 * dump a list of tables:
 * {"dumps": ["foo", "bar"]}
 *
 * 2 dump format = ['json', 'csv']
 * default dump format is 'csv'
 *
 * this command returns the number of records in a table:
 * {"count": "table_name"}
 * {"count": "table_name", "filter": "foo='bar'"}
 *
 * the json blob will push to ecpr-cud-ccce exchange/queue for count command and it look like this:
 *
 * {"customer": "foo",
 * "environment": "p",
 * "uuid": "...",
 * "type": "count",
 * "table": "PRONOTES",
 * "value": 123456}
 *
 * general select query command use to run an sql select directly on cpr or caretend db
 * and upload results (csv or json) to s3. Only select query is allowed.
 *
 * default value of result_type is csv
 *
 * {"run_query": "select * from HR", "result_file": "select_all_HR"}
 * {"run_query": "select * from OT", "result_file": "select_all_OT", "result_type": "json"}
 *
 */

namespace CPREnvoy
{
    public partial class ServiceControlCenter : AMQPControlCenter
    {
        private Logger logger;
        private Config eConfig;
        private Envoy objManager;

        private PerformanceCounter cpuCounter;
        private PerformanceCounter ramCounter;

        class PingResponseData
        {
            public string type { get; set; }
            public decimal memory_usage { get; set; }
            public decimal cpu_usage { get; set; }
            public string db_connection_status { get; set; }
            public string uuid { get; set; }
        }

        public ServiceControlCenter(Config eConfig, Logger logger)
            :base(eConfig, logger, "control")
        {
            this.logger = logger;
            this.eConfig = eConfig;
            this.consumerCallback += (model, args) =>
            {
                string cmdStr = Encoding.UTF8.GetString(args.Body);
                ulong deliveryTag = args.DeliveryTag;
#if (DEBUG)
                Console.WriteLine("{0} - {1}", cmdStr, deliveryTag);
#endif
                try
                {
                    dynamic deserializeObject = JsonConvert.DeserializeObject(cmdStr);
                    this.procesCommand(deserializeObject, deliveryTag);
                }
                catch (Exception error)
                {
                    logger.write( String.Format("Cannot execute the command {0} because {1}", cmdStr, error.Message), Logger.LOGLEVEL.ERROR);
                    this.logger.write( ECPRERROR.ExceptionDetail(error), Logger.LOGLEVEL.ERROR);
                }

                this.consumerBasicACK(deliveryTag);
            };

            this.consumerInit();

            this.cpuCounter = new PerformanceCounter("Process", "% Processor Time", Process.GetCurrentProcess().ProcessName);
            this.ramCounter = new PerformanceCounter("Process", "Working Set - Private", Process.GetCurrentProcess().ProcessName);

        }

        private void procesCommand(dynamic cmdObj, ulong deliveryTag)
        {
            if (cmdObj.config != null)
            {
                string newConfigURL = cmdObj.config;
                this.logger.write( String.Format("Change ecpr config to {0}, it need to be restarted the window service.", newConfigURL), Logger.LOGLEVEL.INFO);
                this.consumerBasicACK(deliveryTag);
                this.eConfig.changeConfigURL(newConfigURL);
                this.objManager.dbTablesCleanUp();
                this.logger.write( "RESTARTING SERVICE !!!", Logger.LOGLEVEL.INFO);
                Environment.Exit(1);
            }
            else if (cmdObj.admin != null)
            {
                string subCommand = cmdObj.admin;
                switch (subCommand)
                {
                    case "schema":
                        if (this.objManager != null)
                            this.objManager.dbSchemaPush();
                        break;
                    case "dump_schema":
                        if (this.objManager != null)
                            this.objManager.dbSchemaDump();
                        break;
                    case "fullsync":
                        if (this.objManager != null)
                            this.objManager.dbDataPush();
                        break;
                    case "restart":
                        this.consumerBasicACK(deliveryTag);
                        restartService();
                        break;
                    case "reload":
                        this.consumerBasicACK(deliveryTag);
                        reloadService();
                        restartService();
                        break;
                    case "timezone":
                        this.get_UTC_timezone();
                        break;
                    case "ping":
                        System.Threading.Tasks.Task.Factory.StartNew(() =>
                        {
                            var pingRespData = new PingResponseData();
                            string errorMessage;
                            pingRespData.type = "pong";
                            pingRespData.memory_usage = Math.Round((decimal)((this.ramCounter.NextValue() / 1024) / 1024), 2);
                            pingRespData.cpu_usage = Math.Round((decimal)this.cpuCounter.NextValue(), 2);
                            pingRespData.db_connection_status = Utils.try_connect_to_db(this.eConfig.cprDatabaseConnectionString, out errorMessage) ? "GOOD" : "DISCONNECTED";
                            pingRespData.uuid = cmdObj.uuid != null ? cmdObj.uuid : Guid.NewGuid().ToString();
                            this.publishMessage(JsonConvert.SerializeObject(pingRespData), "ecpr-config-response", 1);
                        });
                        break;
                    case "update":
                        string service_name = ConfigurationManager.AppSettings.Get("ecpr_service_name");

                        int command_code = 255;
                        if (service_name.Contains("stable"))
                            command_code = 128;
                        else if (service_name.Contains("demo"))
                            command_code = 129;
                        else if (service_name.Contains("sandbox"))
                            command_code = 130;
                        else if (service_name.Contains("beach"))
                            command_code = 131;
                        else if (service_name.Contains("testing"))
                            command_code = 132;
                        else if (service_name.Contains("unstable"))
                            command_code = 133;

                        Console.WriteLine(service_name);
                        Console.WriteLine(command_code);

                        try
                        {
                            ServiceController serviceController = new ServiceController("ECPRAutoUpdate");
                            serviceController.ExecuteCommand(command_code);

                            this.logger.write( "Sent update command to ECPR Auto Update service.");
                        }
                        catch (Exception error)
                        {
                            this.logger.write( String.Format("Cannot run update ecpr because {0}", error.Message), Logger.LOGLEVEL.ERROR);
                            this.logger.write( ECPRERROR.ExceptionDetail(error), Logger.LOGLEVEL.ERROR);
                        }

                        break;
                    default:
                        logger.write( String.Format("Invalid admin command {0}", subCommand), Logger.LOGLEVEL.ERROR);
                        break;
                }
            }
            else if (cmdObj.sync != null)
            {
                string tableName = cmdObj.sync;
                this.objManager.dbDataPush(tableName, cmdObj.filter);
            }
            else if (cmdObj.schema != null)
            {
                string tableName = cmdObj.schema;
                this.objManager.dbSchemaPush(tableName);
            } else if (cmdObj.dump != null)
            {
                string tableName = cmdObj.dump;
                List<string> exclude_columns = null;
                string filterCond = null;

                if (tableName.Trim() == String.Empty)
                    logger.write( String.Format("Invalid dump command: {0}", cmdObj), Logger.LOGLEVEL.ERROR);
                else
                {
                    if (cmdObj.exclude_columns != null)
                        exclude_columns = cmdObj.exclude_columns.ToObject<List<string>>();

                    if (cmdObj.filter != null)
                        filterCond = cmdObj.filter;

                    this.objManager.dumpTable(new List<string> { tableName },
                        exclude_columns,
                        exportFormat(cmdObj), filterCond);
                }
            } else if (cmdObj.dumps != null)
            {
                List<string> listTables = cmdObj.dumps.ToObject<List<string>>();

                if (listTables == null || listTables.Count == 0)
                    logger.write( String.Format("Invalid dumps command: {0}", cmdObj), Logger.LOGLEVEL.ERROR);
                else
                    this.objManager.dumpTable(listTables,
                        null,
                        exportFormat(cmdObj));
            } else if (cmdObj.count != null)
            {
                string tableName = cmdObj.count;
                this.objManager.countTable(tableName, cmdObj.filter != null ? (string)cmdObj.filter : String.Empty);
            } else if (cmdObj.run_query != null)
            {
                string runQuery = cmdObj.run_query;
                runQuery = runQuery.Trim();

                // only select query is allowed
                if (runQuery.StartsWith("select", StringComparison.OrdinalIgnoreCase))
                {
                    string resultFile = String.Empty;
                    if (cmdObj.result_file != null)
                    {
                        resultFile = cmdObj.result_file;
                        resultFile = resultFile.Trim();
                    }
                    else
                    {
                        logger.write( "result_file is required for run_query command.", Logger.LOGLEVEL.ERROR);
                        return;
                    }

                    string resultType = "csv";
                    if (cmdObj.result_type != null)
                    {
                        resultType = cmdObj.result_type;
                        resultType = resultType.Trim().ToLower();
                    }

                    if (resultType != "csv" && resultType != "json")
                    {
                        logger.write( String.Format("result_type = {0} is not supported for run_query command.", resultType), Logger.LOGLEVEL.ERROR);
                        return;
                    }

                    resultFile = String.Format("{0}_{1}.{2}", resultFile, DateTime.Now.ToString("MM-dd-yyyy_HH-mm-ss"), resultType);

                    this.objManager.runQueryToFile(runQuery, resultFile, resultType);
                }
                else
                    logger.write( "Only select query is allowed in run_query command.", Logger.LOGLEVEL.ERROR);
            }
            else
            {
                logger.write( String.Format("Unknown or Invalid command {0}", cmdObj), Logger.LOGLEVEL.ERROR);
            }
        }

        public void addManager(Envoy objManager)
        {
            this.objManager = objManager;
        }

        private void restartService()
        {
            this.logger.write( "RESTARTING SERVICE !!!", Logger.LOGLEVEL.INFO);
            Thread.Sleep(5000);
            Environment.Exit(1);
        }

        private void reloadService()
        {
            this.logger.write( "RELOADING SERVICE !!!", Logger.LOGLEVEL.INFO);
            // stop all rabbitmq consumers/publishers. e.g: ebridged, ecpr-cud, ...
            this.objManager.stopConsumersPublishers();
            // delete all triggers and log tables.
            this.objManager.deleteTriggeraAndLogTables();
        }

        private string exportFormat(dynamic cmdObj)
        {
            string export_format = "csv";
            if (cmdObj.format != null)
                export_format = cmdObj.format;
            return export_format;
        }

        private void get_UTC_timezone()
        {
            /* @returns:
             *
             * {
             *  "type": "timezone",
             *  "data": {
             *  "offset": "-08:00:00",
             *  "utc_now": "02/16/2016 06:28:06 AM",
             *  "now": "02/15/2016 10:28:06 PM"
             *  }
             *  }
             *
             */

            var data = new Dictionary<string, object>();
            var df = "MM/dd/yyyy hh:mm:ss tt";

            var offset = TimeZoneInfo.Local.GetUtcOffset(DateTime.UtcNow);
            data["offset"] = offset;

            /*
             * Get the date and time for the current moment expressed as coordinated
             * universal time (UTC).
             */
            data["utc_now"] = DateTime.UtcNow.ToString(df);

            /*
             * Get the date and time for the current moment, adjusted to the local
             * time zone.
             */
            data["now"] = DateTime.Now.ToString(df);

            Dictionary<string, dynamic> pMsg = new Dictionary<string, dynamic> {
                                                            {"type", "timezone"},
                                                            {"data", data}
                                                        };
            DataChangesCache.AddItem("ADMIN:UTC_TIMEZONE",
                JsonConvert.SerializeObject(pMsg, Formatting.Indented));
        }
    }
}
