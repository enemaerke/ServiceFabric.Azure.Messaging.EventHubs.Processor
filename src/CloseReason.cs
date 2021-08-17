namespace ServiceFabric.Azure.Messaging.EventHubs.Processor
{
    /// <summary>
    /// Why the event processor is being shut down.
    /// </summary>
    public enum CloseReason
    {
        /// <summary>
        /// It was cancelled by Service Fabric.
        /// </summary>
        Cancelled,

        /// <summary>
        /// There was an event hubs failure.
        /// </summary>
        Failure
    }
}
