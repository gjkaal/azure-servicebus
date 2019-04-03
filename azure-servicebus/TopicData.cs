using System;

namespace Nice2Experience.ServiceBus
{
    public class TopicData<T>
    {
        public TopicData(string subject, T data, string version)
        {
            Id = Guid.NewGuid().ToString();
            EventType = typeof(T).ToString();
            Subject = subject;
            EventTime = DateTime.UtcNow.ToString("yyyy-MM-ddThh:mm:ss+00:00");
            DataVersion = version;
            Data = data;
        }
        public string Id { get; set; }
        public string EventType { get; set; }
        public string Subject { get; set; }
        public string EventTime { get; set; }
        public T Data { get; set; }
        public string DataVersion { get; set; }
    }
}