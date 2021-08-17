using System;

namespace ServiceFabric.Azure.Messaging.EventHubs.Processor
{
    /// <summary>
    /// Encapsulating the logging, allow for delegating out to consumers logging system
    /// </summary>
    public class EventProcessorLogging
    {
        public Action<string> OnMessage { get; set; }

        internal void Message(string message)
        {
            OnMessage?.Invoke(message);
        }
    }
}