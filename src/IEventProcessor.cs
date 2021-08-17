using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.EventHubs;

namespace ServiceFabric.Azure.Messaging.EventHubs.Processor
{
    /// <summary>
    /// Interface for processing events.
    /// </summary>
    public abstract class IEventProcessor
    {
        /// <summary>
        /// Called on startup.
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public abstract Task OpenAsync(CancellationToken cancellationToken, PartitionContext context);

        /// <summary>
        /// Called on shutdown.
        /// </summary>
        /// <param name="context"></param>
        /// <param name="reason"></param>
        /// <returns></returns>
        public abstract Task CloseAsync(PartitionContext context, CloseReason reason);

        /// <summary>
        /// Called when events are available.
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <param name="context"></param>
        /// <param name="events"></param>
        /// <returns></returns>
        public abstract Task ProcessEventsAsync(CancellationToken cancellationToken, PartitionContext context, IEnumerable<EventData> events);

        /// <summary>
        /// Called when an error occurs.
        /// </summary>
        /// <param name="context"></param>
        /// <param name="error"></param>
        /// <returns></returns>
        public abstract Task ProcessErrorAsync(PartitionContext context, Exception error);
    }
}
