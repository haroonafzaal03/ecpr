using System;
using System.IO;
using System.Diagnostics;
using System.Linq;
using System.Configuration;
using RabbitMQ.Client;
using System.Text;
using System.Collections.Generic;
using Newtonsoft.Json;

namespace CPREnvoy
{

    public partial class LoggerAMQP : ConnectionFactory
    {
        private string amqpExchange_Name = String.Empty;
        private string amqpPublishQueueName = String.Empty;
        private string amqpPublishRountingKey = String.Empty;

        private int networkRecoveryInterval = 30;
        private ushort requestedHeartbeat = 360;

        public IConnection iConnection = null;
        private IModel iModel = null;

        public bool isConnecting = false;
        public bool isSubscribed = false;

        public LoggerAMQP(Config config)
        {
#if (DEBUG)
            Console.WriteLine("Init LoggerAMQP");
#endif
            this.Uri = new System.Uri(config.amqpUriBrokerServer("control"));
            this.amqpExchange_Name = String.Format("ecpr-log-{0}", config.ccce);
            this.amqpPublishQueueName = this.amqpExchange_Name;
            this.amqpPublishRountingKey = "ecpr-log";

            this.AutomaticRecoveryEnabled = true;
            this.NetworkRecoveryInterval = TimeSpan.FromSeconds(this.networkRecoveryInterval);
            this.RequestedHeartbeat = this.requestedHeartbeat;

            this.ClientProperties = new Dictionary<string, object>()
            {
                {"product", string.Format("{0} logger", Program.serviceName) },
                {"version", Program.getVersion() }
            };

        }

        public void ensureConnect()
        {
            this.isConnecting = true;

            try
            {
                this.iConnection = this.CreateConnection();
                this.iModel = this.iConnection.CreateModel();
                this.iModel.ConfirmSelect();

                isSubscribed = true;
            }
            catch (Exception error)
            {
                EventLogger.write(error.Message, EventLogger.EntryType.ERROR);
#if (DEBUG)
                Console.WriteLine(error.Message);
#endif
            }

            this.isConnecting = false;
        }

        public void push(string logmsg, Logger.LOGLEVEL loglevel)
        {
            if (this.iConnection == null || !this.iConnection.IsOpen)
                return;

            IBasicProperties iBasicProperties = this.iModel.CreateBasicProperties();
            iBasicProperties.ContentType = "application/json";
            iBasicProperties.DeliveryMode = 2;

            Dictionary<string, dynamic> content = new Dictionary<string, dynamic> {
                                                            {"type", "log"},
                                                            {"level", loglevel.ToString()},
                                                            {"timestamp", Utils.getUnixTimeStamp()},
                                                            {"uuid", Guid.NewGuid().ToString() },
                                                            {"content", logmsg}
                                                        };

            try
            {
                this.iModel.BasicPublish(this.amqpExchange_Name,
                    this.amqpPublishRountingKey, iBasicProperties,
                    Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(content, Formatting.Indented)));

                this.iModel.WaitForConfirmsOrDie();
            }
            catch (Exception error)
            {
                EventLogger.write(error.Message, EventLogger.EntryType.ERROR);
#if (DEBUG)
                Console.WriteLine(error.Message);
#endif
            }
        }

    }

    public static class EventLogger
    {
        private static string sourceName = ConfigurationManager.AppSettings.Get("ecpr_service_name");
        private static string logName = ConfigurationManager.AppSettings.Get("ecpr_service_name");

        public enum EntryType {
            ERROR = EventLogEntryType.Error,
            INFO = EventLogEntryType.Information,
            WARN = EventLogEntryType.Warning
        };

        public static void write(string sEvent, EntryType eType = EntryType.ERROR)
        {
            try
            {
                if (!EventLog.SourceExists(sourceName))
                {
                    EventLog.CreateEventSource(sourceName, logName);
                }

                int eventID = eType == EntryType.ERROR ? 1000 : eType == EntryType.WARN ? 1001 : 1002;

                EventLog.WriteEntry(sourceName, sEvent, (EventLogEntryType)eType, eventID);
            }catch(Exception error)
            {
                Console.WriteLine("{0} {1}", sourceName, error.Message);
            }
        }
    }

    public class Logger: LoggerAMQP
    {
        private string log_level;

        public enum LOGLEVEL { TRACE, DEBUG, INFO, WARN, ERROR, FATAL };

        public Logger(Config config): base(config)
        {
            this.log_level = config.log_level;
        }

        public void Dispose()
        {
            this.iConnection.Dispose();
        }

        public void write(string sLogs, LOGLEVEL logLEVEL = LOGLEVEL.DEBUG)
        {
            try
            {
                Console.WriteLine(sLogs);

                var sLogEntryType = EventLogger.EntryType.ERROR;

                if (logLEVEL == LOGLEVEL.INFO)
                    sLogEntryType = EventLogger.EntryType.INFO;

                try
                {
                    EventLogger.write(sLogs, sLogEntryType);
                }
                catch (Exception error)
                {
                    Console.WriteLine(error.Message);
                };

                try
                {
                    if (this.log_level.Contains(logLEVEL.ToString()))
                    {
                        this.push(sLogs, logLEVEL);
                    }
                }
                catch (Exception error)
                {
                    Console.WriteLine(error.Message);
                };
            }
            catch (Exception error)
            {
                Console.WriteLine(error.Message);
            }
        }

    }
}
