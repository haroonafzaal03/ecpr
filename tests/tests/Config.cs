using System;
using System.Net;
using System.Configuration;
using System.Text;
using Newtonsoft.Json;
using System.Collections.Generic;
using Newtonsoft.Json.Linq;
using System.IO;
using System.Reflection;
using System.Threading;

/* example:
{
  "database":
    {
      "ssl": false,
      "server":"dcrxcpr",
      "name":"CPRTEST",
      "username":"cpr_user",
      "password":"cpruser"},
      "tables":
      {
        "HR":
        [
          "MRN",
          "FIRST_NAME",
          "LAST_NAME",
          "ADDRESS",
          "CITY",
          "STATE",
          "ZIP",
          "ADDRESS2",
          "CITY2",
          "STATE2",
          "ZIP2",
          "PHONE",
          "COUNTY",
          "DOB",
          "SSN",
          "SEX",
          "LANGUAGE"
          ]
      },
    "json_mapping":
    {
       "HR":
       {
         "MRN": "external_id"
       }
    },
    "dump_ignore": {
        "TICKCI": ["SIGNATURE"]
    },
    "amqp":
    {
        "server": {
            "protocol": "amqps",
            "host": "envpmq001.envoymobile.net",
            "port": "5671"
        },
        "vhost": {
            "sync_module":
                    {
                        "name": "sync",
                        "exchange_name": "sync",
                        "queue_name": "ecpr-cud,ebridge-cud"
                    },
            "control_module":
                    {
                        "name": "control",
                        "exchange_name": "control",
                        "queue_name": "ecpr-config"
                    }
        },
        "account": {
            "username": "ecpr",
            "password": "Test@1"
        }
    }
}

With ETL:

...
      "sync_module": {
        "name": "sync",
        "exchange_name": "sync-etl,sync",
        "queue_name": "ecpr-cud-*,ebridge-cud"
      },
...

*/

namespace CPREnvoy
{

    public class Config
    {
        private string defaultConfigURL = "https://dist.envoymobile.net/ecpr/config";
        private string defaultBasicAuthUsername = "envoy";
        private string defaultBasicAuthPassword = "d8336184095d";
        private string defaultDatabaseServer = "WIN-P4ME76KMDGR";
        private string defaultDatabaseName = "cpr_test";
        private string defaultDatabaseUsername = null;
        private string defaultDatabasePassword = null;
        public int    defaultDatabaseConnectionTimeout = 15;
        private string defaultUseEncrypt = "Encrypt=True;TrustServerCertificate=True";
        private string defaultDatabaseConnectionString = "Server={0};Database={1};User ID={2};Password={3};Connection Timeout={4};MultipleActiveResultSets=True;ConnectRetryCount=0;Max Pool Size={5}";
        private string defaultDatabaseConnectionStringWindowsAuthentication = "Server={0};Database={1};Integrated Security=SSPI;Connection Timeout={2};MultipleActiveResultSets=True;ConnectRetryCount=0;Max Pool Size={3}";
        private int defaultMaxPoolSize = 5;
        private string defaultLocalConfigFile = "{0}\\config.json";
        private string rootPath;
        private string localConfigFilePath;

        public string restAPIUrl = "";
        public string restAPIBasicAuthUsername = "";
        public string restAPIBasicAuthPassword = "";
        public Dictionary<string, string> restListAPIUri = null;
        public Dictionary<string, Dictionary<string, string>> restListAPIJSONMapping = null;

        public Dictionary<string, List<string>> cprTables = null;
        public List<string> cprTableOrder = null;

        private int maxRetries = 60;
        private bool sslEncryption = true;

        private string defaultAMQPProtocol = "amqps";
        private string defaultAMQPHost = "envpmq001.envoymobile.net";
        private string defaultAMQPPort = "5671";
        private string defaultAMQPUsername = "test";
        private string defaultAMQPPassword = "Test@1";

        private string defaultAMQPVHostSync = "sync";
        private string defaultAMQPVHostControl = "control";
        private string defaultAMQPSyncExchangeName = "sync";
        private string defaultAMQPControlExchangeName = "control";
        private string defaultAMQPSyncQueueName = "";
        private string defaultAMQPControlQueueName = "";

        private string amqpURI = "{0}://{1}:{2}@{3}:{4}/{5}";

        private List<string> currentTables = null;
        private List<string> oldTables = null;

        public Dictionary<string, JObject> diff_tables = null;
        public UInt32 diff_max = 0;
        public DateTime diff_stop = DateTime.UtcNow;

        public bool is_Fetched_OK = false;

        // "DEBUG,ERROR,FATAL,INFO,TRACE,WARN"
        public string log_level = "ERROR,INFO,WARN";

        public int wait_before_start_from_windows_restart = 60;

        public int try_again_after = 30;

        public Dictionary<string, JObject> ignore_fields = null;

        /*
         * There are lot of columns have data type = varchar(max) cause
         * of many problem with encoding/decoding make sure we can
         * includes them in the export file or just ignore.
         *
         */

        public Dictionary<string, List<string>> dumpIgnoreFields = null;

        /* This flag will let ecpr known that this is Radius only customer,
         * so don't need to subscribe to ebridged-cud queue to receive CUD
         * messages
         */

        public bool radius_only_customer = false;

        /*
         * If this option is TRUE then all things (triggers, log tables, etc...)
         * will be deleted and re-create follow the new configs when the service
         * get stopped/started or restarted.
         *
         */
        public bool autoCleanupIfRestart = false;

        public int defaultSQLQueryTimeout = 300;

        public Config()
        {
            var ecpr_config_url = ConfigurationManager.AppSettings["ecpr_config_url"];
            if (ecpr_config_url != null)
            {
                defaultConfigURL = ecpr_config_url;
            }

            this.rootPath = Path.GetDirectoryName(System.Reflection.Assembly.GetEntryAssembly().Location);
            this.localConfigFilePath = String.Format(this.defaultLocalConfigFile, this.rootPath);

            if (!tryToReadConfigFromLocalFile())
                ensurePullConfig();

            this.getTablesInCurrentConfig();
            this.getTablesInOldConfig();
            this.getBasicAuth();

#if (DEBUG)
            if (this.currentTables != null)
            {
                Console.WriteLine("Current Tables: {0}", string.Join(",", this.currentTables.ToArray()));
                if (this.oldTables != null)
                    Console.WriteLine("Old Tables: {0}", string.Join(",", this.oldTables.ToArray()));
            }
#endif
        }

        public List<string> removeTriggerTables()
        {
            if (this.oldTables != null)
            {
                List<string> removeTables = new List<string>();
                foreach (string tableName in this.oldTables)
                {
                    if (!this.currentTables.Contains(tableName))
                    {
                        removeTables.Add(tableName);
                    }
                }
                return removeTables;
            }
            return null;
        }

        public string amqpUriBrokerServer(string cName)
        {
            string vHost = this.defaultAMQPVHostSync;
            if (cName == "control")
                vHost = this.defaultAMQPVHostControl;

            return String.Format(amqpURI,
                this.defaultAMQPProtocol,
                this.defaultAMQPUsername,
                this.defaultAMQPPassword,
                this.defaultAMQPHost,
                this.defaultAMQPPort,
                System.Uri.EscapeDataString(vHost));
        }

        public string amqpExchangeName(string cName)
        {
            if (cName == "sync")
                return this.defaultAMQPSyncExchangeName;

            return this.defaultAMQPControlExchangeName;
        }

        public string amqpQueueName(string cName)
        {
            if (cName == "sync")
                return this.defaultAMQPSyncQueueName;

            return this.defaultAMQPControlQueueName;
        }

        public string cprDatabaseConnectionString
        {
            get
            {
                string connectionString = String.Format(defaultDatabaseConnectionString,
                        this.defaultDatabaseServer,
                        this.defaultDatabaseName,
                        this.defaultDatabaseUsername,
                        this.defaultDatabasePassword,
                        this.defaultDatabaseConnectionTimeout,
                        this.defaultMaxPoolSize);

                if (defaultDatabaseUsername == null && defaultDatabasePassword == null)
                {
                    connectionString = String.Format(defaultDatabaseConnectionStringWindowsAuthentication,
                        this.defaultDatabaseServer,
                        this.defaultDatabaseName,
                        this.defaultDatabaseConnectionTimeout,
                        this.defaultMaxPoolSize);
                }

                if (sslEncryption)
                    connectionString += ";" + defaultUseEncrypt;

                return connectionString;
            }
        }

        public bool needToFetchSchemaAndData
        {
            set
            {
                string bVal = "false";
                if (value)
                    bVal = "true";
                saveAppConfig("ecpr_is_fetched", bVal);
            }
            get
            {
                string ecpr_is_fetched = ConfigurationManager.AppSettings.Get("ecpr_is_fetched");
                return ecpr_is_fetched == "true";
            }
        }

        public void saveDefaultConfigURL()
        {
            saveAppConfig("ecpr_config_url", this.defaultConfigURL);
        }

        public string cprDatabase { get { return defaultDatabaseName; } }

        public string cprQueueName { get { return "CPRPCAQueue"; } }

        public string cprServiceName { get { return "CPRPCAService"; } }

        public string cprUsername { get { return defaultDatabaseUsername; } }

        public string ccce { get { return String.Format("{0}{1}",
            ConfigurationManager.AppSettings["ecpr_customer_code"].ToLower(),
            ConfigurationManager.AppSettings["ecpr_customer_environment"].ToLower()); } }

        public string cprCustomerCode { get { return ConfigurationManager.AppSettings["ecpr_customer_code"].ToLower(); } }

        public string cprCustomerEnvironment { get { return ConfigurationManager.AppSettings["ecpr_customer_environment"].ToLower(); } }

        private void saveAppConfig(string key, string val)
        {
            Configuration appSettings = ConfigurationManager.OpenMappedExeConfiguration(new ExeConfigurationFileMap()
            {
                ExeConfigFilename = String.Format("{0}/ECPR.exe.config", this.rootPath)
            }, ConfigurationUserLevel.None);
            appSettings.AppSettings.Settings[key].Value = val;
            appSettings.Save();
            ConfigurationManager.RefreshSection("appSettings");
        }

        private void configParser(dynamic jsonConfigObject)
        {
            if (jsonConfigObject.autocleanupifrestart != null)
                this.autoCleanupIfRestart = jsonConfigObject.autocleanupifrestart;

            if (jsonConfigObject.database != null)
            {
                try{
                    this.defaultDatabaseServer = jsonConfigObject.database["server"];
                    this.defaultDatabaseName = jsonConfigObject.database["name"];
                    if (jsonConfigObject.database.username != null)
                        this.defaultDatabaseUsername = jsonConfigObject.database["username"];
                    if (jsonConfigObject.database.password != null)
                        this.defaultDatabasePassword = jsonConfigObject.database["password"];
                    if (jsonConfigObject.database.ssl != null)
                        this.sslEncryption = jsonConfigObject.database["ssl"];
                }
                catch (System.Collections.Generic.KeyNotFoundException)
                {
                    EventLogger.write("Invalid CPR+ database config, please re-check ecpr json config.", EventLogger.EntryType.ERROR);
                    Environment.Exit(1);
                }

            }

            if (jsonConfigObject.tables != null)
            {
                JObject jObject = jsonConfigObject.tables;
                this.cprTables = jObject.ToObject<Dictionary<string, List<string>>>();
            }

            if (jsonConfigObject.table_order != null)
            {
                JArray jArray = jsonConfigObject.table_order;
                this.cprTableOrder = jArray.ToObject<List<string>>();
            }
            else
            {
                this.cprTableOrder = new List<string>(this.cprTables.Keys);
            }

            if (jsonConfigObject.rest_api != null)
            {
                this.restAPIUrl = jsonConfigObject.rest_api["baseurl"];
                this.restAPIBasicAuthUsername = jsonConfigObject.rest_api["username"];
                this.restAPIBasicAuthPassword = jsonConfigObject.rest_api["password"];

                JObject jObject = jsonConfigObject.rest_api["apiuri"];
                this.restListAPIUri = jObject.ToObject<Dictionary<string, string>>();
            }

            if (jsonConfigObject.json_mapping != null)
            {
                JObject jObject = jsonConfigObject.json_mapping;
                Dictionary<string, JObject> pL = jObject.ToObject<Dictionary<string, JObject>>();
                this.restListAPIJSONMapping = new Dictionary<string, Dictionary<string, string>>();
                foreach (string pKey in pL.Keys)
                {
                    Dictionary<string, string> pVal = new Dictionary<string,string>();
                    pVal = pL[pKey].ToObject<Dictionary<string, string>>();
                    this.restListAPIJSONMapping.Add(pKey, pVal);
                }
            }

            if (jsonConfigObject.amqp != null)
            {
                try
                {
                    JObject jObject = jsonConfigObject.amqp["server"];
                    Dictionary<string, string> jServer = jObject.ToObject<Dictionary<string, string>>();
                    this.defaultAMQPProtocol = jServer["protocol"];
                    this.defaultAMQPHost = jServer["host"];
                    this.defaultAMQPPort = jServer["port"];

                    jObject = jsonConfigObject.amqp["account"];
                    jServer = jObject.ToObject<Dictionary<string, string>>();
                    this.defaultAMQPUsername = jServer["username"];
                    this.defaultAMQPPassword = jServer["password"];

                    jObject = jsonConfigObject.amqp["vhost"];
                    Dictionary<string, JObject> pL = jObject.ToObject<Dictionary<string, JObject>>();

                    jServer = pL["sync_module"].ToObject<Dictionary<string, string>>();
                    this.defaultAMQPVHostSync = jServer["name"];
                    this.defaultAMQPSyncExchangeName = jServer["exchange_name"];
                    this.defaultAMQPSyncQueueName = jServer["queue_name"];

                    jServer = pL["control_module"].ToObject<Dictionary<string, string>>();
                    this.defaultAMQPVHostControl = jServer["name"];
                    this.defaultAMQPControlExchangeName = jServer["exchange_name"];
                    this.defaultAMQPControlQueueName = jServer["queue_name"];

                }
                catch (System.Collections.Generic.KeyNotFoundException)
                {
                    EventLogger.write("Invalid AMQP config, please re-check ecpr json config.", EventLogger.EntryType.ERROR);
                    Environment.Exit(1);
                }
            }

            if (jsonConfigObject.diff != null)
            {
                try
                {
                    JObject jObject = jsonConfigObject.diff["tables"];
                    this.diff_tables = jObject.ToObject<Dictionary<string, JObject>>();
                    this.diff_max = jsonConfigObject.diff["max"];
                    this.diff_stop = DateTime.ParseExact((string)jsonConfigObject.diff["stop"], "MM/dd/yyyy HH:mm",
                        System.Globalization.CultureInfo.InvariantCulture);
                }
                catch(Exception error)
                {
                    var error_str = String.Format("Cannot load diff tables config because {0}, please re-check ecpr json config.", error.Message);
#if (DEBUG)
                    Console.WriteLine(error_str);
#endif
                    EventLogger.write(error_str, EventLogger.EntryType.ERROR);
                }
            }

            if (jsonConfigObject.db_connection_timeout != null)
            {
                this.defaultDatabaseConnectionTimeout = (int)jsonConfigObject.db_connection_timeout;
            }

            if (jsonConfigObject.sql_query_timeout != null)
            {
                this.defaultSQLQueryTimeout = (int)jsonConfigObject.sql_query_timeout;
            }

            if (jsonConfigObject.log_level != null)
            {
                this.log_level = (string)jsonConfigObject.log_level;
                this.log_level = this.log_level.ToUpper();
            }

            if (jsonConfigObject.wait_before_start_from_windows_restart != null)
            {
                this.wait_before_start_from_windows_restart = (int)jsonConfigObject.wait_before_start_from_windows_restart;
            }

            if (jsonConfigObject.ignore != null)
            {
                JObject jObject = jsonConfigObject.ignore;
                this.ignore_fields = jObject.ToObject<Dictionary<string, JObject>>();
            }

            if (jsonConfigObject.radius_only_customer != null)
            {
                this.radius_only_customer = (bool)jsonConfigObject.radius_only_customer;
            }

            if (jsonConfigObject.dump_ignore != null) {
                JObject jObject = jsonConfigObject.dump_ignore;
                this.dumpIgnoreFields = jObject.ToObject<Dictionary<string, List<string>>>();
            } else
            {
                this.dumpIgnoreFields = new Dictionary<string, List<string>>() {
                    { "TICKCI", new List<string>(){ "SIGNATURE" } }
                };
            }

            this.defaultMaxPoolSize = cprTables.Count;

            this.is_Fetched_OK = true;
        }

        private int sleepTime(int retryCount)
        {
            switch (retryCount)
            {
                case 0: return 0;
                case 1: return 3000;
                case 2: return 6000;
                case 3: return 10000;
                case 4: return 20000;
                case 5: return 30000;
                default:
                    return 40000;
            }
        }

        private void getBasicAuth()
        {
            string ecpr_http_basic_auth_username = ConfigurationManager.AppSettings.Get("ecpr_http_basic_auth_username");
            string ecpr_http_basic_auth_password = ConfigurationManager.AppSettings.Get("ecpr_http_basic_auth_password");
            if (ecpr_http_basic_auth_username != null && ecpr_http_basic_auth_password != null)
            {
                this.defaultBasicAuthUsername = ecpr_http_basic_auth_username.Trim();
                this.defaultBasicAuthPassword = ecpr_http_basic_auth_password.Trim();
            } else
            {
                string error = "No basic authenticate account setup for fetch ECPR config in ECPR.exe.config.";
#if (DEBUG)
                Console.WriteLine(error);
#endif
                EventLogger.write(error, EventLogger.EntryType.ERROR);
            }
            string info = string.Format("Basic authenticate account used to fetch config: {0}:{1}", this.defaultBasicAuthUsername, this.defaultBasicAuthPassword);
#if (DEBUG)
            Console.WriteLine(info);
#endif
            EventLogger.write(info, EventLogger.EntryType.INFO);
        }

        private void ensurePullConfig()
        {
            bool isDone = false;
            int attempts = 0;

            this.getBasicAuth();

            while (!isDone)
            {
                attempts++;

                string msg01 = String.Format("Getting config from {0}", this.defaultConfigURL);
                Console.WriteLine(msg01);
                try
                {
                    EventLogger.write(msg01, EventLogger.EntryType.INFO);
                }catch(Exception){};

                HttpWebRequest httpWebRequest = (HttpWebRequest)WebRequest.Create(defaultConfigURL);
                httpWebRequest.Accept = "application/json";

                String _encoded = System.Convert.ToBase64String(
                    System.Text.Encoding.GetEncoding("ISO-8859-1").GetBytes(
                    this.defaultBasicAuthUsername + ":" + this.defaultBasicAuthPassword));

                httpWebRequest.Headers.Add("Authorization", "Basic " + _encoded);

                try
                {
                    HttpWebResponse httpWebResponse = (HttpWebResponse)httpWebRequest.GetResponse();
                    if (httpWebResponse.StatusCode == HttpStatusCode.OK)
                    {
                        using (var reader = new System.IO.StreamReader(httpWebResponse.GetResponseStream(), ASCIIEncoding.UTF8))
                        {
                            string responseText = reader.ReadToEnd();
                            File.WriteAllText(@localConfigFilePath, responseText);
                            this.configParser(JsonConvert.DeserializeObject(responseText));
                            break;
                        }
                    }
                }
                catch (WebException webException)
                {

                    if (attempts >= maxRetries)
                    {
                        isDone = true;
                    }

                    string msg = String.Format("An error has occurred when getting config from server: {0}, retry in {1} secs.", webException.Message, this.sleepTime(attempts)/1000);
                    Console.WriteLine(msg);
                    try
                    {
                        EventLogger.write(msg, EventLogger.EntryType.ERROR);
                    }
                    catch (Exception) { };
                    Thread.Sleep(this.sleepTime(attempts));
                }

            }
        }

        public void changeConfigURL(string newConfigURL)
        {
            //FIXME: Need to also change `cprpca_config_url` value in App.config
            //Backup current config file: config.json -> config.json.bak
            if (File.Exists(localConfigFilePath))
            {
                File.Copy(localConfigFilePath, String.Format("{0}/config.json.bak", this.rootPath), true);
            }
            this.defaultConfigURL = newConfigURL;
            //Change config in config.json
            ensurePullConfig();
        }

        private bool tryToReadConfigFromLocalFile()
        {
            if (File.Exists(localConfigFilePath))
            {
                try
                {
                    string configObject = File.ReadAllText(@localConfigFilePath);
                    this.configParser(JsonConvert.DeserializeObject(configObject));

                    string msg = String.Format("Successful config load from local ({0}).", localConfigFilePath);
                    Console.WriteLine(msg);
                    try
                    {
                        EventLogger.write(msg, EventLogger.EntryType.INFO);
                    }
                    catch (Exception) { };
                }
                catch (Exception e)
                {
                    string msg = String.Format("Cannot read {0} because {1}. Will pull config from server.", localConfigFilePath, e.ToString());
                    Console.WriteLine(msg);
                    try
                    {
                        EventLogger.write(msg, EventLogger.EntryType.ERROR);
                    }
                    catch (Exception) { };
                    return false;
                }
                return true;
            }
            else
            {
                string msg = String.Format("The config file {0} not found!. Will pull config from server.", localConfigFilePath);
                Console.WriteLine(msg);
                try{
                    EventLogger.write(msg, EventLogger.EntryType.INFO);
                }
                catch (Exception) { };

                return false;
            }
        }

        private void getTablesInCurrentConfig()
        {
            if (File.Exists(localConfigFilePath))
            {
                string configObject = File.ReadAllText(@localConfigFilePath);
                dynamic jsonObject = JsonConvert.DeserializeObject(configObject);
                Dictionary<string, object> tablesObject = jsonObject.tables.ToObject<Dictionary<string, object>>();
                if (tablesObject.Keys.Count > 0)
                {
                    this.currentTables = new List<string>(tablesObject.Keys);
                    return;
                }
            }

            this.currentTables = null;
        }

        private void getTablesInOldConfig()
        {
            string oldConfigFile = String.Format("{0}/config.json.bak", this.rootPath);
            if (File.Exists(oldConfigFile))
            {
                string configObject = File.ReadAllText(@oldConfigFile);
                dynamic jsonObject = JsonConvert.DeserializeObject(configObject);
                Dictionary<string, object> tablesObject = jsonObject.tables.ToObject<Dictionary<string, object>>();
                if (tablesObject.Keys.Count > 0)
                {
                    this.oldTables = new List<string>(tablesObject.Keys);
                    return;
                }
            }

            this.oldTables = null;
        }
    }
}
