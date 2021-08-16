namespace Azure.Messaging.EventHubs.ServiceFabricProcessor
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
