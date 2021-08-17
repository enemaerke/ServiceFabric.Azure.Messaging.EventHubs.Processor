using System;
using Azure.Messaging.EventHubs.Consumer;

namespace Azure.Messaging.EventHubs.ServiceFabricProcessor
{
    /// <summary>
    /// Type used for OnShutdown property.
    /// </summary>
    /// <param name="e"></param>
    public delegate void ShutdownNotification(Exception e);

    /// <summary>
    /// Options that govern the functioning of the processor.
    /// </summary>
    public class EventProcessorOptions
    {
        /// <summary>
        /// Construct with default options.
        /// </summary>
        public EventProcessorOptions()
        {
            this.MaxBatchSize = 10;
            this.PrefetchCount = 300;
            this.ReceiveTimeout = TimeSpan.FromMinutes(1);
            this.InvokeProcessorAfterReceiveTimeout = false;
            this.InitialPositionProvider = partitionId => EventPosition.Earliest;
            this.OnShutdown = null;
        }

        /// <summary>
        /// The maximum number of events that will be presented to IEventProcessor.OnEventsAsync in one call.
        /// Defaults to 10.
        /// </summary>
        public int MaxBatchSize { get; set; }

        /// <summary>
        /// The prefetch count for the Event Hubs receiver.
        /// Defaults to 300.
        /// </summary>
        public int PrefetchCount { get; set; }

        /// <summary>
        /// The timeout for the Event Hubs receiver.
        /// Defaults to one minute.
        /// </summary>
        public TimeSpan ReceiveTimeout { get; set; }

        /// <summary>
        /// Determines whether IEventProcessor.OnEventsAsync is called when the Event Hubs receiver times out.
        /// Set to true to get calls with empty event list.
        /// Set to false to not get calls.
        /// Defaults to false.
        /// </summary>
        public bool InvokeProcessorAfterReceiveTimeout { get; set; }

        /// <summary>
        /// If there is no checkpoint, the user can provide a position for the Event Hubs receiver to start at.
        /// Defaults to first event available in the stream.
        /// </summary>
        public Func<string, EventPosition> InitialPositionProvider { get; set; }

        /// <summary>
        /// Exposes a logging interface
        /// </summary>
        public EventProcessorLogging Logging { get; } = new EventProcessorLogging();

        /// <summary>
        /// TODO -- is this needed? It's called just before SFP.RunAsync throws out/returns to user code anyway.
        /// But user code won't see that until it awaits the Task, so maybe this is useful?
        /// </summary>
        public ShutdownNotification OnShutdown { get; set; }

        internal void NotifyOnShutdown(Exception shutdownException)
        {
            if (this.OnShutdown != null)
            {
                this.OnShutdown(shutdownException);
            }
        }
    }
}
