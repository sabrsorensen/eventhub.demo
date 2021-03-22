using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.HttpsPolicy;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.OpenApi.Models;
using Newtonsoft.Json;

namespace eventhub.sender
{
    public interface IMessageProducer<T>
    {
        Task Send(T payload);
    }

    public class EventHubMessageProducer<T> : IMessageProducer<T>
    {
        private readonly EventHubProducerClient _producerClient;
        public EventHubMessageProducer(EventHubProducerClient producerClient)
        {
            _producerClient = producerClient;
        }
        public async Task Send(T payload)
        {
            var serializedPayload = JsonConvert.SerializeObject(payload);
            using EventDataBatch eventBatch = await _producerClient.CreateBatchAsync();

            // Add events to the batch. An event is a represented by a collection of bytes and metadata. 
            eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(serializedPayload)));

            // Use the producer client to send the batch of events to the event hub
            await _producerClient.SendAsync(eventBatch);
        }
    }

    public class Startup
    {
        public Startup(IConfiguration configuration)
        {
            Configuration = configuration;
        }

        public IConfiguration Configuration { get; }

        private async Task ConfigureEventHub()
        {
            //string _eventHubConnectionString = Configuration["EventHub:ConnectionString"];
            string _eventHubConnectionString = Configuration["EventHub:DirectConnectionString"];
            string _eventHubName = Configuration["EventHub:Name"];
            // Create a producer client that you can use to send events to an event hub
            await using (var producerClient = new EventHubProducerClient(_eventHubConnectionString, _eventHubName))
            {
                var dumb = new { Id = "1", Name = "Jerry", Impact = 1.10m, Test = true };
                var messageProducer = new EventHubMessageProducer<Object>(producerClient);
                await messageProducer.Send(dumb);
            }
        }
        // This method gets called by the runtime. Use this method to add services to the container.
        public void ConfigureServices(IServiceCollection services)
        {

            services.AddControllers();
            services.AddSwaggerGen(c =>
            {
                c.SwaggerDoc("v1", new OpenApiInfo { Title = "eventhub.sender", Version = "v1" });
            });
            ConfigureEventHub().Wait();
        }

        // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        public void Configure(IApplicationBuilder app, IWebHostEnvironment env)
        {
            if (env.IsDevelopment())
            {
                app.UseDeveloperExceptionPage();
                app.UseSwagger();
                app.UseSwaggerUI(c => c.SwaggerEndpoint("/swagger/v1/swagger.json", "eventhub.sender v1"));
            }

            app.UseHttpsRedirection();

            app.UseRouting();

            app.UseAuthorization();

            app.UseEndpoints(endpoints =>
            {
                endpoints.MapControllers();
            });
        }
    }
}
