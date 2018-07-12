using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CPREnvoy
{
    public partial class ECPR_RabbitMQ : ConnectionFactory
    {
        private IConnection iConnection = null;
        private IModel iModel = null;
        private EventingBasicConsumer eventingBasicConsumer = null;

        private int networkRecoveryInterval = 30;
        private ushort requestedHeartbeat = 360;
        private bool is_consumer = false;
        private int numberMessages = 0;
        private string exchangeName;

        public ECPR_RabbitMQ(Config config, bool is_consumer = true)
        {
            this.Uri = new System.Uri(config.amqpUriBrokerServer("sync"));

            this.AutomaticRecoveryEnabled = true;
            this.NetworkRecoveryInterval = System.TimeSpan.FromSeconds(this.networkRecoveryInterval);
            this.RequestedHeartbeat = this.requestedHeartbeat;

            this.is_consumer = is_consumer;
            this.exchangeName = config.amqpExchangeName("sync").Split(',').GetValue(1).ToString();
        }

        public bool publishMessage(string msgBody, string rounting_key = "")
        {
            IBasicProperties iBasicProperties = this.iModel.CreateBasicProperties();
            iBasicProperties.ContentType = "application/json";
            iBasicProperties.DeliveryMode = 2;

            this.iModel.BasicPublish(this.exchangeName, rounting_key, iBasicProperties, Encoding.UTF8.GetBytes(msgBody));
            this.iModel.WaitForConfirmsOrDie();

            return true;
        }

        public void consumerInit(List<string> queues)
        {
            if (this.iConnection == null || !this.iConnection.IsOpen)
                return;

            eventingBasicConsumer = new EventingBasicConsumer(this.iModel);
            eventingBasicConsumer.Received += this.consumerCallback;

            this.iModel.BasicQos(0, 1, false); // prefetchCount = 1

            try
            {
                foreach (string queue in queues)
                {
                    this.iModel.BasicConsume(queue, false, eventingBasicConsumer);
                }
            }
            catch (Exception error)
            {
                Console.WriteLine(error);
            }
        }

        private void consumerCallback(object sender, BasicDeliverEventArgs args)
        {
            string message = Encoding.UTF8.GetString(args.Body);
            ulong deliveryTag = args.DeliveryTag;
            string queue = args.RoutingKey;

            this.consumerBasicACK(deliveryTag);

#if DEBUG
            Console.WriteLine(args.RoutingKey);
            Console.WriteLine(deliveryTag);
            Console.WriteLine(message);
#endif

            dynamic jsonData = JsonConvert.DeserializeObject(message);

            string table_name = jsonData.source;

            numberMessages += 1;

#if DEBUG
            Console.Write("{0} ", numberMessages);
#endif
        }

        private void consumerBasicACK(ulong deliveryTag)
        {
            try
            {
                this.iModel.BasicAck(deliveryTag, false);
            }
            catch (Exception error)
            {
                Console.WriteLine(error);
            }
        }

        private void consumerBasicNACK(ulong deliveryTag)
        {
            try
            {
                this.iModel.BasicNack(deliveryTag, false, true);
            }
            catch (Exception error)
            {
                Console.WriteLine(error);
            }
        }

        public void getConnect(List<string> queues)
        {
#if DEBUG
            Console.WriteLine("Connecting to rabbitmq: {0}", this.Uri, this.is_consumer ? "CONSUMER": "PUBLISHER");
#endif

            try
            {
                this.iConnection = this.CreateConnection();
                this.iModel = this.iConnection.CreateModel();
                this.iModel.ConfirmSelect();

                foreach (string queue in queues)
                {
                    this.iModel.QueueBind(queue, this.exchangeName, queue);
                }

            }
            catch (Exception error)
            {
#if DEBUG
                Console.WriteLine("ERROR: {0}", error);
#endif
            }
        }

    }

    abstract public class CUD_Queue
    {
        public Config config;

        public JsonSerializerSettings JSS = new JsonSerializerSettings()
        {
            Formatting = Formatting.Indented,
        };
    } 

    public class Test_ECPR_CUD_Queue: CUD_Queue
    {
        public Test_ECPR_CUD_Queue(Config config)
        {
            this.config = config;
        }

        public void runTests()
        {
            var q = new ECPR_RabbitMQ(this.config);
            var x = new List<string>() { "ecpr-cud-ebridge" };
            q.getConnect(x);
            q.consumerInit(x);
        }
    }

    public class Test_EBRIGDED_CUD_Queue: CUD_Queue
    {
        class GeneralEbridgeMsg
        {
            public string source { get; set; }
            public string uuid
            {
                get { return Guid.NewGuid().ToString(); }
            }
        }

        class EbridgeDeleteMsg : GeneralEbridgeMsg
        {
            public string type = "delete";
            public string key { get; set; }
            public string value { get; set; }
        }

        class EbridgeInsertMsg : GeneralEbridgeMsg
        {
            public string type = "insert";
            public Dictionary<string, object> data { get; set; }
        }

        class EbridgeUpdateMsg : GeneralEbridgeMsg
        {
            public string type = "update";
            public string key { get; set; }
            public string value { get; set; }
            public Dictionary<string, object> data { get; set; }
            public string last_seen { get; set; }
        }

        class EbridgeUpsertMsg : EbridgeUpdateMsg { }

        private int numberMessages;
        private enum MsgType { INSERT, UPDATE, UPSERT, DELETE};

        public Test_EBRIGDED_CUD_Queue(Config config, int numberMessages=100)
        {
            this.config = config;
            this.numberMessages = numberMessages;
        }

        public void runInsertTest()
        {
            runTests(() => {
                return buildMessages();
            });
        }

        public void runUpdateTest()
        {
            runTests(() => {
                return buildMessages(MsgType.UPDATE);
            });
        }

        public void runUpsertTest()
        {
            runTests(() => {
                return buildMessages(MsgType.UPSERT);
            });
        }

        public void runDeleteTest()
        {
            runTests(() => {
                return buildMessages(MsgType.DELETE);
            });
        }

        private void runTests(Func<List<object>> callbackFN)
        {
            var q = new ECPR_RabbitMQ(this.config, false);
            var x = new List<string>() { "ebridged-cud" };
            q.getConnect(x);

            var data = callbackFN();

            int i = 0;
            Task[] taskSendMsg = new Task[data.Count];

            foreach (var msg in data)
            {
                taskSendMsg[i] = Task.Factory.StartNew(() =>
                {
                    q.publishMessage(JsonConvert.SerializeObject(msg, this.JSS), "ebridged-cud");
                });

                i += 1;
            }

            Task.WaitAll(taskSendMsg);
        }

        public void countTables()
        {
            foreach (var table in TestQuery.TEST_QUERY)
            {
                var rows = TestDB.runSQLQuery(config, String.Format("SELECT COUNT(*) AS COUNT FROM {0}", table.Key));
                var count = rows.First();

                Console.WriteLine("Table: {0} - Count: {1}", table.Key, count.Value);
            }
        }

        private List<object> buildMessages(MsgType msgType = MsgType.INSERT)
        {
            var messages = new List<object>();

            foreach (var table in TestQuery.TEST_QUERY)
            {
                for (var i = 1; i <= this.numberMessages; i++)
                {
                    var data = new Dictionary<string, object>();

                    foreach(var v in table.Value)
                    {
                        data.Add(v, i.ToString());
                    }

                    object message;

                    switch (msgType)
                    {
                        case MsgType.INSERT:
                            var insertMessage = new EbridgeInsertMsg();
                            insertMessage.source = table.Key;
                            insertMessage.data = data;
                            message = insertMessage;
                            break;
                        case MsgType.UPDATE:
                            var updateMessage = new EbridgeUpdateMsg();
                            updateMessage.source = table.Key;
                            message = updateMessage;
                            break;
                        case MsgType.UPSERT:
                            var upsertMessage = new EbridgeUpsertMsg();
                            upsertMessage.source = table.Key;
                            message = upsertMessage;
                            break;
                        default:
                            var deleteMessage = new EbridgeDeleteMsg();
                            deleteMessage.source = table.Key;
                            message = deleteMessage;
                            break;
                    }

                    messages.Add(message);
                }
            }

            return messages;
        }
    }
}
