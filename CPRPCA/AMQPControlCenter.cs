using System;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Net.NetworkInformation;
using System.Linq;
using System.Text;
using RabbitMQ.Client.Exceptions;
using System.Collections.Generic;

namespace CPREnvoy
{
    public partial class AMQPControlCenter : ConnectionFactory
    {
        private string amqpExchange_Name = String.Empty;
        private string amqpPublishQueueName = String.Empty;
        private string amqpSubscribeQueueName = String.Empty;
        private string amqpPublishExchange_Name = String.Empty;

        private IConnection iConnection = null;
        private IModel iModel = null;
        private IModel iModelResponse = null;
        private Logger logger;

        private EventingBasicConsumer eventingBasicConsumer = null;
        private string consumerTag = String.Empty;
        private string moduleType;
        private string ccce;

        private int networkRecoveryInterval = 30;
        private ushort requestedHeartbeat = 360;

        public EventHandler<RabbitMQ.Client.Events.BasicDeliverEventArgs> consumerCallback = null;
        public bool isSubscribed = false;
        public bool isConnecting = false;

        public string mRountingKey = "";

        public AMQPControlCenter(Config eConfig, Logger logger, string moduleType)
        {

            this.logger = logger;
            this.Uri = new System.Uri(eConfig.amqpUriBrokerServer(moduleType));
            this.moduleType = moduleType;
            this.ccce = eConfig.ccce;

            string queueName = eConfig.amqpQueueName(moduleType).Trim();
            string exchangeName = eConfig.amqpExchangeName(moduleType).Trim();

            if (queueName.Contains(','))
            {
                string[] queues = queueName.Split(',');
                this.amqpPublishQueueName = queues[0]; // First queue name should be ecpr-*
                this.amqpSubscribeQueueName = queues[1]; // Second queue name should be ebridge-*
            }
            else
            {
                this.amqpPublishQueueName = queueName;
                this.amqpSubscribeQueueName = queueName;
            }

            if (exchangeName.Contains(','))
            {
                string[] exchanges = exchangeName.Split(',');
                this.amqpPublishExchange_Name = exchanges[0]; //First exchange is fanout exchange
                this.amqpExchange_Name = exchanges[1]; //Second exchange is topic exchange
            }
            else
                this.amqpExchange_Name = exchangeName;

            this.ClientProperties = this.ECPRClientProperties();

            this.AutomaticRecoveryEnabled = true;
            this.NetworkRecoveryInterval = TimeSpan.FromSeconds(this.networkRecoveryInterval);
            this.RequestedHeartbeat = this.requestedHeartbeat;

            NetworkChange.NetworkAddressChanged += new NetworkAddressChangedEventHandler(NetworkChange_NetworkAddressChanged);
            NetworkChange.NetworkAvailabilityChanged += new NetworkAvailabilityChangedEventHandler(NetworkChange_NetworkAvailabilityChanged);

#if (DEBUG)
            Console.WriteLine("Init {0} AMQP control center ({1}).", this.amqpExchange_Name, eConfig.amqpUriBrokerServer(moduleType));
#endif

            this.reConnect();

        }

        private IDictionary<string, object> ECPRClientProperties()
        {
            IDictionary<string, object> clientProperties = new Dictionary<string, object>();
            clientProperties["product"] = Program.serviceName;
            clientProperties["version"] = Program.getVersion();
            return clientProperties;
        }

        private void NetworkChange_NetworkAvailabilityChanged(object sender, NetworkAvailabilityEventArgs e)
        {
#if (DEBUG)
            Console.WriteLine("ALERT: Network Availability Changed");
#endif
        }

        private void NetworkChange_NetworkAddressChanged(object sender, EventArgs e)
        {
#if (DEBUG)
            Console.WriteLine("ALERT: Network Address Changed");
#endif
        }

        public void reConnect()
        {
            this.isConnecting = true;

            logger.write( "Connecting to amqp broker server ...", Logger.LOGLEVEL.INFO);

            try
            {
                this.iConnection = this.CreateConnection();
                this.iModel = this.iConnection.CreateModel();

                if (moduleType == "sync")
                {
                    this.iModel.ConfirmSelect();

                    if (!this.amqpPublishQueueName.Contains("*"))
                        this.iModel.QueueBind(this.amqpPublishQueueName, this.amqpExchange_Name, "");

                    this.iModel.BasicAcks += new EventHandler<BasicAckEventArgs>(iModel_BasicAcks);
                    this.iModel.BasicRecoverOk += new EventHandler<EventArgs>(iModel_BasicRecoverOk);
                }
                else
                if (moduleType == "control")
                {
                    this.iModel.ConfirmSelect();
                    this.iModel.QueueBind(this.amqpPublishQueueName, this.amqpExchange_Name, "ecpr-config");
                    this.iModel.BasicAcks += new EventHandler<BasicAckEventArgs>(iModel_BasicAcks);
                    this.iModel.BasicRecoverOk += new EventHandler<EventArgs>(iModel_BasicRecoverOk);

                    this.iModelResponse = this.iConnection.CreateModel();
                    this.iModelResponse.ConfirmSelect();
                }

                logger.write( "Connected to amqp broker server.", Logger.LOGLEVEL.INFO);

                //this.consumerInit();

                isSubscribed = true;
            }
            catch (Exception e)
            {
                logger.write( String.Format("ERROR: An error has occurred or cannot connect to the amqp broker server ({0}). Please re-check the config.", e.Message), Logger.LOGLEVEL.ERROR);
            }

            this.isConnecting = false;
        }

        public bool publishMessage(string msgBody, string rounting_key="", int modelType=0)
        {

            if (this.iConnection == null || !this.iConnection.IsOpen)
            {
                return false;
            }

            IBasicProperties iBasicProperties = this.iModel.CreateBasicProperties();
            iBasicProperties.ContentType = "application/json";
            iBasicProperties.DeliveryMode = 2;

            try
            {
                if (modelType == 0)
                {
                    var exchangeName = this.amqpExchange_Name;
                    if (this.moduleType == "sync" && !this.amqpPublishExchange_Name.Equals(String.Empty))
                    {
                        exchangeName = this.amqpPublishExchange_Name;
                        rounting_key = exchangeName;
                    }

                    this.iModel.BasicPublish(exchangeName, rounting_key, iBasicProperties, Encoding.UTF8.GetBytes(msgBody));
                    this.iModel.WaitForConfirmsOrDie();

                } else
                {
                    var exchangeName = String.Format("ecpr-config-response-{0}", this.ccce);
                    var routing_key = modelType == 1 ? "ecpr-config-response" : "ecpr-config-s3-response";

                    this.iModelResponse.BasicPublish(exchangeName, routing_key, iBasicProperties, Encoding.UTF8.GetBytes(msgBody));
                    this.iModelResponse.WaitForConfirmsOrDie();

                }

                return true;
            }
            catch (OperationInterruptedException)
            {
                this.logger.write( "RabbitMQ connection is closed, will try to re-open in " + (this.networkRecoveryInterval) + " secs!", Logger.LOGLEVEL.ERROR);
            }
            catch (Exception e)
            {
                logger.write( e.Message, Logger.LOGLEVEL.ERROR);
            }

            return false;
        }

        private void iModel_BasicAcks(object sender, BasicAckEventArgs e)
        {

        }

        private void iModel_BasicRecoverOk(object sender, EventArgs e)
        {

        }

        public void consumerInit(ushort prefetchCount=1)
        {
            if (this.iConnection == null || !this.iConnection.IsOpen)
                return;

            eventingBasicConsumer = new EventingBasicConsumer(this.iModel);

            /* Sequential processing for groups of messages
             * Send ACK when the message get processed.
             */
            this.iModel.BasicQos(0, prefetchCount, false);

            eventingBasicConsumer.Received += this.consumerCallback;

            try
            {
                this.consumerTag = this.iModel.BasicConsume(this.amqpSubscribeQueueName, false, eventingBasicConsumer);
            }
            catch (Exception e)
            {
                logger.write( e.ToString(), Logger.LOGLEVEL.ERROR);
            }
        }

        protected void consumerBasicACK(ulong deliveryTag)
        {
            try
            {
                this.iModel.BasicAck(deliveryTag, false);
            }
            catch (Exception e)
            {
                logger.write( e.ToString(), Logger.LOGLEVEL.ERROR);
            }
        }

        protected void consumerBasicNACK(ulong deliveryTag)
        {
            try
            {
                this.iModel.BasicNack(deliveryTag, false, true);
            }
            catch (Exception e)
            {
                logger.write( e.ToString(), Logger.LOGLEVEL.ERROR);
            }
        }

        public void Dispose()
        {
            this.iConnection.Dispose();
        }

        ~AMQPControlCenter()
        {
            if (this.iConnection == null || !this.iConnection.IsOpen)
                return;

            if (this.iModel.IsOpen)
            {
                if (this.consumerTag != String.Empty)
                {
                    try
                    {
                        this.iModel.BasicCancel(this.consumerTag);
                    }catch(Exception error)
                    {
                        Console.WriteLine(error.Message);
                    }
                }
                try
                {
                    this.iModel.Close();
                }catch(Exception error)
                {
                    Console.WriteLine(error.Message);
                }
            }

        }
    }
}
