using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Consumer;
using Azure.Messaging.EventHubs.Primitives;
using Microsoft.ServiceFabric.Data;

namespace ServiceFabric.Azure.Messaging.EventHubs.Processor
{
    /// <summary>
    /// Base class that implements event processor functionality.
    /// </summary>
    public class ServiceFabricProcessor
    {
        // Service Fabric objects initialized in constructor
        private readonly Uri serviceFabricServiceName;
        private readonly Guid serviceFabricPartitionId;

        // ServiceFabricProcessor settings initialized in constructor
        private readonly IEventProcessor userEventProcessor;
        private readonly EventProcessorOptions options;
        private readonly ICheckpointManager checkpointManager;

        // EventHub settings initialized in constructor
        private readonly string consumerGroupName;
        private readonly EventHubConnection eventHubConnection;

        // Initialized during RunAsync startup
        private PartitionContext partitionContext;
        private CancellationTokenSource internalCancellationTokenSource;
        private CancellationToken linkedCancellationToken;

        // Value managed by RunAsync
        private int running;

        /// <summary>
        /// Constructor. Arguments break down into three groups: (1) Service Fabric objects so this library can access
        /// Service Fabric facilities, (2) Event Hub-related arguments which indicate what event hub to receive from and
        /// how to process the events, and (3) advanced, which right now consists only of the ability to replace the default
        /// reliable dictionary-based checkpoint manager with a user-provided implementation.
        /// </summary>
        /// <param name="serviceFabricServiceName">Service Fabric Uri found in StatefulServiceContext</param>
        /// <param name="serviceFabricPartitionId">Service Fabric partition id found in StatefulServiceContext</param>
        /// <param name="stateManager">Service Fabric-provided state manager, provides access to reliable dictionaries</param>
        /// <param name="userEventProcessor">User's event processor implementation</param>
        /// <param name="eventHubConnectionString">Connection string for user's event hub</param>
        /// <param name="eventHubConsumerGroup">Name of event hub consumer group to receive from</param>
        /// <param name="options">Optional: Options structure for ServiceFabricProcessor library</param>
        /// <param name="checkpointManager">Very advanced/optional: user-provided checkpoint manager implementation</param>
        public ServiceFabricProcessor(
            Uri serviceFabricServiceName,
            Guid serviceFabricPartitionId,
            IReliableStateManager stateManager,
            IEventProcessor userEventProcessor,
            string eventHubConnectionString,
            string eventHubConsumerGroup,
            EventProcessorOptions options = null,
            ICheckpointManager checkpointManager = null)
        {
            if (serviceFabricServiceName == null)
            {
                throw new ArgumentNullException(nameof(serviceFabricServiceName));
            }

            if (serviceFabricPartitionId == null)
            {
                throw new ArgumentNullException(nameof(serviceFabricPartitionId));
            }

            if (stateManager == null)
            {
                throw new ArgumentNullException(nameof(stateManager));
            }

            if (string.IsNullOrEmpty(eventHubConnectionString))
            {
                throw new ArgumentException("eventHubConnectionString is null or empty");
            }
            if (string.IsNullOrEmpty(eventHubConsumerGroup))
            {
                throw new ArgumentException("eventHubConsumerGroup is null or empty");
            }

            this.serviceFabricServiceName = serviceFabricServiceName;
            this.serviceFabricPartitionId = serviceFabricPartitionId;

            this.userEventProcessor = userEventProcessor ?? throw new ArgumentNullException(nameof(userEventProcessor));

            this.eventHubConnection = new EventHubConnection(eventHubConnectionString);
            this.consumerGroupName = eventHubConsumerGroup;

            this.options = options ?? new EventProcessorOptions();
            this.checkpointManager = checkpointManager ?? new ReliableDictionaryCheckpointManager(stateManager, this.options.Logging);
        }

        /// <summary>
        /// Starts processing of events.
        /// </summary>
        /// <param name="fabricCancellationToken">Cancellation token provided by Service Fabric, assumed to indicate instance shutdown when cancelled.</param>
        /// <returns>Task that completes when event processing shuts down.</returns>
        public async Task RunAsync(CancellationToken fabricCancellationToken)
        {
            if (Interlocked.Exchange(ref this.running, 1) == 1)
            {
                options.Logging?.Message("Already running");
                throw new InvalidOperationException("EventProcessorService.RunAsync has already been called.");
            }

            this.internalCancellationTokenSource = new CancellationTokenSource();

            try
            {
                using (CancellationTokenSource cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(fabricCancellationToken, this.internalCancellationTokenSource.Token))
                {
                    this.linkedCancellationToken = cancellationTokenSource.Token;
                    
                    await InnerRunAsync().ConfigureAwait(false);

                    this.options.NotifyOnShutdown(null);
                }
            }
            catch (Exception e)
            {
                // If InnerRunAsync throws, that is intended to be a fatal exception for this instance.
                // Catch it here just long enough to log and notify, then rethrow.

                options.Logging?.Message($"THROWING OUT: {e}");
                if (e.InnerException != null)
                {
                    options.Logging?.Message($"THROWING OUT INNER: {e.InnerException}");
                }
                this.options.NotifyOnShutdown(e);

                throw;
            }
        }

        private async Task InnerRunAsync()
        {
            bool processorOpened = false;
            EventHubConsumerClient consumerClient = null;

            try
            {
                //
                // Get Service Fabric partition information.
                //
                var partitionInfo = await GetServicePartitionId(this.linkedCancellationToken).ConfigureAwait(false);

                //
                // Create EventHubConsumerClient and check partition count.
                //
                consumerClient = CreateConsumerClient();
                var hubPartitionId = GetHubPartitionId(consumerClient, partitionInfo);
                
                //
                // Generate a PartitionContext now that the required info is available.
                //
                this.partitionContext = new PartitionContext(
                    this.linkedCancellationToken,
                    hubPartitionId,
                    this.eventHubConnection.EventHubName,
                    this.consumerGroupName,
                    this.checkpointManager);

                //
                // Start up checkpoint manager and get checkpoint, if any.
                //
                long? initialOffSet = await GetInitialOffsetFromCheckpointAtStartup(hubPartitionId, this.linkedCancellationToken).ConfigureAwait(false);

                //
                // If there was a checkpoint, the offset is in this.initialOffset, so convert it to an EventPosition.
                // If no checkpoint, get starting point from user-supplied provider.
                //
                EventPosition initialPosition = GetEventPosition(initialOffSet, hubPartitionId); 

                //
                // Create batch receiver for the partition.
                //
                var partitionReceiverOptions = new PartitionReceiverOptions()
                {
                    PrefetchCount = options.PrefetchCount,
                    TrackLastEnqueuedEventProperties = true,
                };
                var receiver = new PartitionReceiver(
                    this.consumerGroupName,
                    hubPartitionId,
                    initialPosition,
                    this.eventHubConnection.ToString(),
                    this.eventHubConnection.EventHubName,
                    partitionReceiverOptions);
                try
                {
                    // Call Open on user's event processor instance.
                    // If user's Open code fails, treat that as a fatal exception and let it throw out.
                    //
                    options.Logging?.Message("Creating event processor");
                    await this.userEventProcessor.OpenAsync(this.linkedCancellationToken, this.partitionContext).ConfigureAwait(false);
                    processorOpened = true;
                    options.Logging?.Message("Event processor created and opened OK");

                    while (!linkedCancellationToken.IsCancellationRequested)
                    {
                        IEnumerable<EventData> eventBatch = await receiver.ReceiveBatchAsync(
                            options.MaxBatchSize,
                            options.ReceiveTimeout,
                            linkedCancellationToken);

                        await this.ProcessEventsAsync(hubPartitionId, eventBatch);
                    }
                }
                catch (TaskCanceledException)
                {
                    // This is expected if the cancellation token is
                    // signaled.
                }
                finally
                {
                    await receiver.DisposeAsync();
                }
            }
            finally
            {
                if (processorOpened)
                {
                    try
                    {
                        await this.userEventProcessor.CloseAsync(this.partitionContext, this.linkedCancellationToken.IsCancellationRequested ? CloseReason.Cancelled : CloseReason.Failure).ConfigureAwait(false);
                    }
                    catch (Exception e)
                    {
                        options.Logging?.Message($"IEventProcessor.CloseAsync threw {e}, continuing cleanup");
                    }
                }
                if (consumerClient != null)
                {
                    try
                    {
                        await consumerClient.CloseAsync(CancellationToken.None).ConfigureAwait(false);
                    }
                    catch (Exception e)
                    {
                        options.Logging?.Message($"EventHubClient close threw {e}, continuing cleanup");
                    }
                }
            }
        }

        private EventPosition GetEventPosition(long? offset, string hubPartitionId)
        {
            if (offset != null)
            {
                options.Logging?.Message($"Initial position from checkpoint, offset {offset.Value}");
                return EventPosition.FromOffset(offset.Value);
            }

            options.Logging?.Message("Initial position from provider");
            return this.options.InitialPositionProvider(hubPartitionId);
        }

        private string GetHubPartitionId(EventHubConsumerClient consumerClient, PartitionInformation partitionInfo)
        {
            options.Logging?.Message("Getting event hub info");
            EventHubProperties eventHubProperties = null;

            // Lambda MUST be synchronous to work with RetryWrapper!
            var lastException = RetryWrapper(() => { eventHubProperties = consumerClient.GetEventHubPropertiesAsync(linkedCancellationToken).Result; });
            if (eventHubProperties == null)
            {
                options.Logging?.Message("Out of retries getting event hub info");
                throw new Exception("Out of retries getting event hub runtime info", lastException);
            }
            
            if (eventHubProperties.PartitionIds.Length != partitionInfo.ServicePartitionCount)
            {
                options.Logging?.Message($"Service partition count {partitionInfo.ServicePartitionCount} does not match event hub partition count {eventHubProperties.PartitionIds.Length}");
                throw new EventProcessorConfigurationException($"Service partition count {partitionInfo.ServicePartitionCount} does not match event hub partition count {eventHubProperties.PartitionIds.Length}");
            }

            return eventHubProperties.PartitionIds[partitionInfo.FabricPartitionOrdinal];
        }

        private EventHubConsumerClient CreateConsumerClient()
        {
            EventHubConsumerClient client = null;
            Exception lastException = null;
            options.Logging?.Message("Creating event hub client");
            lastException = RetryWrapper(() =>
            {
                client = new EventHubConsumerClient(
                    this.consumerGroupName,
                    this.eventHubConnection.ToString(),
                    this.eventHubConnection.EventHubName);
            });

            if (client == null)
            {
                options.Logging?.Message("Out of retries event hub client");
                throw new Exception("Out of retries creating EventHubClient", lastException);
            }
            
            options.Logging?.Message("Event hub client OK");
            return client;
        }

        private EventHubsException RetryWrapper(Action action)
        {
            EventHubsException lastException = null;

            for (int i = 0; i < Constants.RetryCount; i++)
            {
                this.linkedCancellationToken.ThrowIfCancellationRequested();
                try
                {
                    action.Invoke();
                    break;
                }
                catch (EventHubsException e)
                {
                    if (!e.IsTransient)
                    {
                        throw e;
                    }
                    lastException = e;
                }
                catch (AggregateException ae)
                {
                    if (ae.InnerException is EventHubsException ehe)
                    {
                        if (!ehe.IsTransient)
                        {
                            throw ehe;
                        }
                        lastException = ehe;
                    }
                    else
                    {
                        throw ae;
                    }
                }
            }

            return lastException;
        }

        async Task ProcessEventsAsync(string hubPartitionId, IEnumerable<EventData> events)
        {
            IEnumerable<EventData> effectiveEvents = events ?? new List<EventData>(); // convert to empty list if events is null
            bool hasEvents = false;

            if (events != null)
            {
                // Save position of last event if we got a real list of events
                IEnumerator<EventData> scanner = effectiveEvents.GetEnumerator();
                EventData last = null;
                while (scanner.MoveNext())
                {
                    hasEvents = true;
                    last = scanner.Current;
                }

                if (last != null)
                {
                    this.partitionContext.SetOffsetAndSequenceNumber(last);
                }
            }

            try
            {
                if (hasEvents)
                {
                    await this.userEventProcessor.ProcessEventsAsync(this.linkedCancellationToken, this.partitionContext, effectiveEvents)
                        .ConfigureAwait(false);
                }
                else if (options.InvokeProcessorAfterReceiveTimeout)
                {
                    await this.userEventProcessor.ProcessEventsAsync(this.linkedCancellationToken, this.partitionContext, Enumerable.Empty<EventData>())
                        .ConfigureAwait(false);
                }
            }
            catch (Exception e)
            {
                options.Logging?.Message($"Processing exception on {hubPartitionId}: {e}");
                SafeProcessError(this.partitionContext, e);
            }
        }

        private void SafeProcessError(PartitionContext context, Exception error)
        {
            try
            {
                this.userEventProcessor.ProcessErrorAsync(context, error).Wait(linkedCancellationToken);
            }
            catch (Exception e)
            {
                // The user's error notification method has thrown.
                // Recursively notifying could easily cause an infinite loop, until the stack runs out.
                // So do not notify, just log.
                options.Logging?.Message($"Error thrown by ProcessErrorASync: {e}");
            }
        }

        private async Task<long?> GetInitialOffsetFromCheckpointAtStartup(string hubPartitionId, CancellationToken cancellationToken)
        {
            // Set up store and get checkpoint, if any.
            await this.checkpointManager.CreateCheckpointStoreIfNotExistsAsync(cancellationToken).ConfigureAwait(false);
            if (options.AttemptCheckpointMigrationFromOldFormat)
            {
                bool migrationSuccess = await this.checkpointManager.AttemptMigration(hubPartitionId, cancellationToken);
                if (migrationSuccess)
                {
                    options.Logging?.Message($"Data migration completed for partition: {hubPartitionId}");
                }
            }

            Checkpoint checkpoint = await this.checkpointManager.CreateCheckpointIfNotExistsAsync(hubPartitionId, cancellationToken).ConfigureAwait(false);
            if (!checkpoint.Valid)
            {
                // Not actually any existing checkpoint.
                options.Logging?.Message("No checkpoint");
                return null;
            }
            
            if (checkpoint.Version == 1)
            {
                options.Logging?.Message($"Checkpoint provides initial offset {checkpoint.Offset}");
                return checkpoint.Offset;
            }
            else
            {
                // It's actually a later-version checkpoint but we don't know the details.
                // Access it via the V1 interface and hope it does something sensible.
                options.Logging?.Message($"Unexpected checkpoint version {checkpoint.Version}, provided initial offset {checkpoint.Offset}");
                return checkpoint.Offset;
            }
        }

        private async Task<PartitionInformation> GetServicePartitionId(CancellationToken cancellationToken)
        {
            IFabricPartitionLister lister = new ServiceFabricPartitionLister();
            var info = new PartitionInformation(
                await lister.GetServiceFabricPartitionCount(this.serviceFabricServiceName).ConfigureAwait(false),
                await lister.GetServiceFabricPartitionOrdinal(this.serviceFabricPartitionId).ConfigureAwait(false));

            options.Logging?.Message($"Total partitions {info.ServicePartitionCount}");
            options.Logging?.Message($"Partition ordinal {info.FabricPartitionOrdinal}");

            return info;
        }

        private readonly struct PartitionInformation
        {
            public int ServicePartitionCount { get; }
            public int FabricPartitionOrdinal { get; }

            public PartitionInformation(int partitionCount, int partitionOrdinal)
            {
                ServicePartitionCount = partitionCount;
                FabricPartitionOrdinal = partitionOrdinal;
            }
        }
    }
}
