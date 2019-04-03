using System;
using System.Text;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.EventHubs.Processor;
using Newtonsoft.Json;

namespace Nice2Experience.ServiceBus
{

    public class EventHubConfiguration
    {
        public EventHubConfiguration()
        {
            EventHubPath = Guid.NewGuid().ToString();
            LeaseContainerName = "leases";
        }
        public string AegSasKey { get; set; }
        public string TopicUrl { get; set; }
        public string Subject { get; set; }

        public string EventHubPath { get; set; }
        public string ConsumerGroupName { get; set; }
        public string EventHubConnectionString { get; set; }
        public string StorageAccountName { get; set; }
        public string StorageAccountKey { get; set; }
        public string StorageConnection => $"DefaultEndpointsProtocol=https;AccountName={StorageAccountName};AccountKey={StorageAccountKey}";
        public string LeaseContainerName { get; set; }
    }

    public class TopicReceiverListener<T, TReceiver> where TReceiver : TopicReceiver<T>, new()
    {
        private EventHubConfiguration _config;
        private EventProcessorHost _eventProcessorHost;
        public TopicReceiverListener(EventHubConfiguration config)
        {
            _config = config;
        }

        public async Task<bool> StartListening()
        {
            if (_eventProcessorHost != null)
            {
                await _eventProcessorHost.UnregisterEventProcessorAsync();
            }
            _eventProcessorHost = new EventProcessorHost(
                 typeof(T).ToString() + "_" + _config.EventHubPath,
                 _config.EventHubConnectionString,
                 _config.ConsumerGroupName,
                 _config.StorageConnection,
                 _config.LeaseContainerName
             );
            await _eventProcessorHost.RegisterEventProcessorAsync<TReceiver>();
            return true;
        }

        public async Task<bool> StopListening()
        {
            if (_eventProcessorHost != null)
            {
                await _eventProcessorHost.UnregisterEventProcessorAsync();
            }
            return true;
        }

    }

    public abstract class TopicReceiver<T> : IEventProcessor
    {

        private string _typeName = typeof(T).ToString();
        public bool Open { get; private set; }
        public bool Error { get; private set; }
        public string ErrorMessage { get; private set; }
        public CloseReason CloseReason { get; set; }
        public void ClearError()
        {
            Error = false;
            ErrorMessage = string.Empty;
        }

        // Called by processor host to indicate that the event processor is being stopped.
        public async Task CloseAsync(PartitionContext context, CloseReason reason)
        {
            // do some winding dowm
            Open = false;
            CloseReason = reason;
            await Task.CompletedTask;
        }

        // Called by processor host to initialize the event processor.
        public async Task OpenAsync(PartitionContext context)
        {
            // do some initializetion
            Open = true;
            await Task.CompletedTask;
        }

        //  This method is provided for informational purposes.
        public async Task ProcessErrorAsync(PartitionContext context, Exception error)
        {
            Error = true;
            ErrorMessage = error.Message;
            await Task.CompletedTask;
        }

        // Called by the processor host when a batch of events has arrived.
        public async Task ProcessEventsAsync(PartitionContext context, IEnumerable<EventData> messages)
        {
            foreach (var item in messages)
            {
                var data = Encoding.UTF8.GetString(item.Body.Array);
                var topic = JsonConvert.DeserializeObject<TopicData<T>>(data);
                if (topic.EventType == _typeName)
                {
                    await HandleTopic(topic);
                }
                // set handled
                await context.CheckpointAsync(item);
            }
        }

        public abstract Task HandleTopic(TopicData<T> topic);
    }

}