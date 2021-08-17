using System;

namespace ServiceFabric.Azure.Messaging.EventHubs.Processor
{
    /// <summary>
    /// Exception thrown when the configuration of the service has a problem.
    /// </summary>
    public class EventProcessorConfigurationException : Exception
    {
        /// <summary>
        /// Construct the exception.
        /// </summary>
        /// <param name="message"></param>
        public EventProcessorConfigurationException(string message) : base(message)
        {
        }
    }
}
