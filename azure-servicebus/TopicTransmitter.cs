using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using System.Collections.Generic;
using Newtonsoft.Json;

namespace Nice2Experience.ServiceBus
{

    public class TopicTransmitter
    {
        private HttpClient client;

        private readonly string _aegSasKey;
        private readonly string _topicUrl;
        private readonly string _subject;

        public TopicTransmitter(string aegSasKey, string topicUrl, string subject)
        {
            _aegSasKey = aegSasKey;
            _topicUrl = topicUrl;
            _subject = subject;
            client = new HttpClient();
        }

        public async Task<HttpStatusCode> SendEvents<T>(params T[] items)
        {
            var topic = new List<TopicData<T>>();
            foreach (var item in items)
            {
                topic.Add(new TopicData<T>(_subject, item, "1.0"));
            }
            var payload = JsonConvert.SerializeObject(topic);
            var topicUrl = _topicUrl + "?api-version=2018-01-01";
            client.DefaultRequestHeaders.Add("aeg-sas-key", _aegSasKey);
            var result = await client.PostAsync(topicUrl, new StringContent(payload));
            return result.StatusCode;
        }


    }

}