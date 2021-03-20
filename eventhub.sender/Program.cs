using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using Azure.Security.KeyVault.Secrets;
using Azure.Identity;
using Azure.Extensions.AspNetCore.Configuration.Secrets;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace eventhub.sender
{
    public class Program
    {
        private string _eventHubConnectionString;
        private string _eventHubName;
        public static void Main(string[] args)
        {

            CreateHostBuilder(args).Build().Run();
        }

        public static IHostBuilder CreateHostBuilder(string[] args) =>
            Host.CreateDefaultBuilder(args)
                .ConfigureAppConfiguration((context, config) =>
                {
                    var builtConfig = config.Build();
                    var secretClient = new SecretClient(new Uri($"https://{builtConfig["KeyVaultName"]}.vault.azure.net/"),
                                                             new DefaultAzureCredential());
                    config.AddAzureKeyVault(secretClient, new KeyVaultSecretManager());
                })
                .ConfigureWebHostDefaults(webBuilder =>
                {
                    webBuilder.UseStartup<Startup>();
                });
    }
}
