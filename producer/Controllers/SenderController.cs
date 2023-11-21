using Confluent.Kafka;
using Microsoft.AspNetCore.Mvc;
using producer.Models;
using System.Text.Json;

// For more information on enabling Web API for empty projects, visit https://go.microsoft.com/fwlink/?LinkID=397860

namespace producer.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class SenderController(
            ILoggerFactory loggerFactory, 
            IConfiguration configuration,
            ProducerConfig producerConfig)
        : ControllerBase
    {
        private readonly ILogger<SenderController> _logger = LoggerFactoryExtensions.CreateLogger<SenderController>(loggerFactory);

        // POST api/<SenderController>
        [HttpPost]
        public async Task<string> Post([FromBody] Order order)
        {
            string resultMessage;

            try
            {
                var topicName = configuration.GetSection("ConfluentCloud:Topic").Value;
                var partition = configuration.GetSection("ConfluentCloud:Partition").Value;

                var orderMessage = JsonSerializer.Serialize(order);

                using var producer = new ProducerBuilder<string, string>(producerConfig).Build();
                // Note: Awaiting the asynchronous produce request below prevents flow of execution
                // from proceeding until the acknowledgement from the broker is received (at the 
                // expense of low throughput).
                var deliveryReport = await producer.ProduceAsync(
                    topicName, new Message<string, string> { Key = partition, Value = orderMessage });

                switch (deliveryReport.Status)
                {
                    case PersistenceStatus.Persisted:
                        resultMessage = $"delivered and acknowledge to: {deliveryReport.TopicPartitionOffset}";
                        break;
                    case PersistenceStatus.PossiblyPersisted:
                        resultMessage = $"delivered but not yet acknowledged to: {deliveryReport.TopicPartitionOffset}";
                        break;
                    default:
                        resultMessage = $"failed to deliver to: {deliveryReport.TopicPartitionOffset}";
                        break;
                }

                return resultMessage;
            }
            catch (ProduceException<string, string> ex)
            {
                resultMessage =
                    $"failed to deliver message: {ex.Message} [{ex.Error.Code}] for message (value: '{ex.DeliveryResult.Value}')";

                _logger.LogError(ex, resultMessage);

                return resultMessage;
            }
        }
    }
}
