using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Consumer;
using Azure.Messaging.EventHubs.Processor;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.HttpsPolicy;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.OpenApi.Models;

namespace eventhub.receiver
{
    public class Startup
    {
        public Startup(IConfiguration configuration)
        {
            Configuration = configuration;
        }

        public IConfiguration Configuration { get; }
        private EventProcessorClient _processor;

        private void ConfigureEventHubProcessor()
        {
             // Read from the default consumer group: $Default
            string consumerGroup = EventHubConsumerClient.DefaultConsumerGroupName;
            string _blobStorageConnectionString = Configuration["AzureBlob:ConnectionString"];
            string _blobContainerName = Configuration["AzureBlob:ContainerName"];
            string _ehubNamespaceConnectionString = Configuration["EventHub:ConnectionString"];
            string _eventHubName = Configuration["EventHub:Name"];

            // Create a blob container client that the event processor will use 
            BlobContainerClient storageClient = new BlobContainerClient(_blobStorageConnectionString, _blobContainerName);

            // Create an event processor client to process events in the event hub
            _processor = new EventProcessorClient(storageClient, consumerGroup, _ehubNamespaceConnectionString, _eventHubName);
            _processor.ProcessEventAsync += _processor_ProcessEventAsync;
            _processor.ProcessErrorAsync += _processor_ProcessErrorAsync;
            _processor.StartProcessing();
        }

        private Task _processor_ProcessErrorAsync(ProcessErrorEventArgs arg)
        {
            return Task.CompletedTask;
        }

        private async Task _processor_ProcessEventAsync(ProcessEventArgs arg)
        {
            var eventData = arg.Data.EventBody.ToString();
            await arg.UpdateCheckpointAsync();
        }

        // This method gets called by the runtime. Use this method to add services to the container.
        public void ConfigureServices(IServiceCollection services)
        {

            services.AddControllers();
            services.AddSwaggerGen(c =>
            {
                c.SwaggerDoc("v1", new OpenApiInfo { Title = "eventhub.receiver", Version = "v1" });
            });
            ConfigureEventHubProcessor();
        }

        // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        public void Configure(IApplicationBuilder app, IWebHostEnvironment env)
        {
            if (env.IsDevelopment())
            {
                app.UseDeveloperExceptionPage();
                app.UseSwagger();
                app.UseSwaggerUI(c => c.SwaggerEndpoint("/swagger/v1/swagger.json", "eventhub.receiver v1"));
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
