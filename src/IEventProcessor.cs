using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Azure.Messaging.EventHubs.ServiceFabricProcessor
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

        /// <summary>
        /// Called periodically to get user-supplied load metrics.
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public virtual Dictionary<string, int> GetLoadMetric(CancellationToken cancellationToken, PartitionContext context)
        {
            // By default all partitions have a metric of named CountOfPartitions with value 1. If Service Fabric is configured to use this metric,
            // it will balance primaries across nodes simply by the number of primaries on a node. This can be overridden to return
            // more sophisticated metrics like number of events processed or CPU usage.
            Dictionary<string, int> defaultMetric = new Dictionary<string, int>();
            defaultMetric.Add(Constants.DefaultUserLoadMetricName, 1);
            return defaultMetric;
        }
    }
}
