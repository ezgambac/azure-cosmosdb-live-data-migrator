namespace MigrationConsoleApp
{
    using System;
    using System.Diagnostics;
    using System.Threading.Tasks;
    using Azure.Storage.Blobs;
    using Microsoft.Azure.Documents;
    using Microsoft.Azure.Documents.ChangeFeedProcessor;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement;
    using Microsoft.Azure.Documents.Client;
    using CommonData;
    using System.Collections.ObjectModel;

    public class ChangeFeedProcessorHost
    {
        private MigrationConfig config;
        private IChangeFeedProcessor changeFeedProcessor;
        private DocumentClient destinationCollectionClient;

        public DocumentClient GetDestinationCollectionClient()
        {
            if (destinationCollectionClient == null)
            {
                destinationCollectionClient = new DocumentClient(
                    new Uri(config.DestUri), config.DestSecretKey,
                new ConnectionPolicy() { ConnectionMode = ConnectionMode.Direct, ConnectionProtocol = Protocol.Tcp },
                ConsistencyLevel.Eventual);
            }

            return destinationCollectionClient;
        }

        public ChangeFeedProcessorHost(MigrationConfig config)
        {
            this.config = config;
        }

        public async Task StartAsync()
        {
            Trace.TraceInformation(
                "Starting monitor(source) collection creation: Url {0} - key {1} - dbName {2} - collectionName {3}",
                this.config.MonitoredUri,
                this.config.MonitoredSecretKey,
                this.config.MonitoredDbName,
                this.config.MonitoredCollectionName);

            await this.CreateCollectionIfNotExistsAsync(
               this.config.MonitoredUri,
               this.config.MonitoredSecretKey,
               this.config.MonitoredDbName,
               this.config.MonitoredCollectionName,
               this.config.MonitoredThroughput,
               this.config.SourcePartitionKeys);

            Trace.TraceInformation(
                "Starting lease (transaction log of change feed) standard collection creation: Url {0} - key {1} - dbName {2} - collectionName {3}",
                this.config.LeaseUri,
                this.config.LeaseSecretKey,
                this.config.LeaseDbName,
                this.config.LeaseCollectionName);

            await this.CreateCollectionIfNotExistsAsync(
                this.config.LeaseUri,
                this.config.LeaseSecretKey,
                this.config.LeaseDbName,
                this.config.LeaseCollectionName,
                this.config.LeaseThroughput,
                "id");

            Trace.TraceInformation(
                "destination (sink) collection : Url {0} - key {1} - dbName {2} - collectionName {3}",
                this.config.DestUri,
                this.config.DestSecretKey,
                this.config.DestDbName,
                this.config.DestCollectionName);

            await this.CreateCollectionIfNotExistsAsync(
                this.config.DestUri,
                this.config.DestSecretKey,
                this.config.DestDbName,
                this.config.DestCollectionName,
                this.config.DestThroughput,
                this.config.TargetPartitionKey);

            await this.RunChangeFeedHostAsync();
        }
        public async Task CreateCollectionIfNotExistsAsync(string endPointUri, string secretKey, string databaseName, string collectionName, int throughput, string partitionKey)
        {
            using (DocumentClient client = new DocumentClient(new Uri(endPointUri), secretKey))
            {
                await client.CreateDatabaseIfNotExistsAsync(new Database { Id = databaseName });

                await client.CreateDocumentCollectionIfNotExistsAsync(
                    UriFactory.CreateDatabaseUri(databaseName),
                    new DocumentCollection() { 
                        Id = collectionName, 
                        PartitionKey = new PartitionKeyDefinition() { Paths = new Collection<string>() { $"/{partitionKey}" } }
                    },
                    new RequestOptions { OfferThroughput = throughput });
            }
        }

        public async Task CloseAsync()
        {
            if(GetDestinationCollectionClient() != null )
            {
                GetDestinationCollectionClient().Dispose();
            }

            if(changeFeedProcessor != null)
            {
                await changeFeedProcessor.StopAsync();
            }

            destinationCollectionClient = null;
            changeFeedProcessor = null;
        }

        public async Task<IChangeFeedProcessor> RunChangeFeedHostAsync()
        {
            string hostName = Guid.NewGuid().ToString();
            Trace.TraceInformation("Host name {0}", hostName);

            // monitored collection info 
            var sourceCollInfo = new DocumentCollectionInfo
            {
                Uri = new Uri(this.config.MonitoredUri),
                MasterKey = this.config.MonitoredSecretKey,
                DatabaseName = this.config.MonitoredDbName,
                CollectionName = this.config.MonitoredCollectionName
            };

            var policy = new ConnectionPolicy()
            {
                ConnectionMode = ConnectionMode.Direct,
                ConnectionProtocol = Protocol.Tcp
            };

            policy.PreferredLocations.Add("North Europe");

            // lease collection info 
            var leaseCollectionInfo = new DocumentCollectionInfo
            {
                Uri = new Uri(this.config.LeaseUri),
                MasterKey = this.config.LeaseSecretKey,
                DatabaseName = this.config.LeaseDbName,
                CollectionName = this.config.LeaseCollectionName,
                ConnectionPolicy = policy
            };

            // destination collection info 
            var destCollInfo = new DocumentCollectionInfo
            {
                Uri = new Uri(this.config.DestUri),
                MasterKey = this.config.DestSecretKey,
                DatabaseName = this.config.DestDbName,
                CollectionName = this.config.DestCollectionName
            };

            var processorOptions = this.GenerateProcessorOptions();

            Trace.TraceInformation("Processor options Starts from Beginning - {0}, Lease renew interval - {1}",
                processorOptions.StartFromBeginning,
                processorOptions.LeaseRenewInterval.ToString());

            var docTransformer = new DefaultDocumentTransformer();

            BlobContainerClient containerClient = null;

            if (!String.IsNullOrEmpty(config.BlobConnectionString))
            {
                BlobServiceClient blobServiceClient = new BlobServiceClient(config.BlobConnectionString);
                containerClient = blobServiceClient.GetBlobContainerClient(config.BlobContainerName);
                await containerClient.CreateIfNotExistsAsync();
            }

            var sourceClient = new DocumentClient(sourceCollInfo.Uri, sourceCollInfo.MasterKey, policy, ConsistencyLevel.Eventual);
            var destClient = new DocumentClient(destCollInfo.Uri, destCollInfo.MasterKey, policy, ConsistencyLevel.Eventual);

            var docObserverFactory = new DocumentFeedObserverFactory(config.SourcePartitionKeys, config.TargetPartitionKey, destClient, destCollInfo, docTransformer, containerClient);

            changeFeedProcessor = await new ChangeFeedProcessorBuilder()
                .WithObserverFactory(docObserverFactory)
                .WithHostName(hostName)
                .WithFeedCollection(sourceCollInfo)
                .WithLeaseCollection(leaseCollectionInfo)
                .WithProcessorOptions(processorOptions)
                .WithFeedDocumentClient(sourceClient)
                .BuildAsync();
            await changeFeedProcessor.StartAsync().ConfigureAwait(false);

            if (config.EnableBackAndForthMigration)
            {
                var reverseProcessorOptions = this.GenerateProcessorOptions();

                var reverseDocObserverFactory = new DocumentFeedObserverFactory(config.TargetPartitionKey, config.SourcePartitionKeys, sourceClient, sourceCollInfo, docTransformer, containerClient);
                var reverseChangeFeedProcessor = await new ChangeFeedProcessorBuilder()
                    .WithObserverFactory(reverseDocObserverFactory)
                    .WithHostName(hostName)
                    .WithFeedCollection(sourceCollInfo)
                    .WithLeaseCollection(leaseCollectionInfo)
                    .WithProcessorOptions(reverseProcessorOptions)
                    .WithFeedDocumentClient(destClient)
                    .BuildAsync();
                await reverseChangeFeedProcessor.StartAsync().ConfigureAwait(false);
            }

            return changeFeedProcessor;
        }

        private ChangeFeedProcessorOptions GenerateProcessorOptions()
        {
            var processorOptions = new ChangeFeedProcessorOptions
            {
                LeaseRenewInterval = TimeSpan.FromSeconds(30),
                MaxItemCount = 1000,
                LeasePrefix = Guid.NewGuid().ToString()
            };
            if (config.DataAgeInHours.HasValue)
            {
                if (config.DataAgeInHours.Value >= 0)
                {
                    processorOptions.StartTime = DateTime.UtcNow.Subtract(TimeSpan.FromHours(config.DataAgeInHours.Value));
                }
            }
            else
            {
                processorOptions.StartFromBeginning = true;
            }

            return processorOptions;
        }
    }
}
