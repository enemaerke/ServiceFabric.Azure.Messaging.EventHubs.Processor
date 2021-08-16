using System;

namespace Azure.Messaging.EventHubs.ServiceFabricProcessor
{
    class Constants
    {
        internal static readonly int RetryCount = 5;

        internal static readonly int FixedReceiverEpoch = 0;

        internal static readonly TimeSpan MetricReportingInterval = TimeSpan.FromMinutes(1.0);
        internal static readonly string DefaultUserLoadMetricName = "CountOfPartitions";

        internal static readonly TimeSpan ReliableDictionaryTimeout = TimeSpan.FromSeconds(10.0); // arbitrary
        internal static readonly string CheckpointDictionaryName = "EventProcessorCheckpointDictionary";
        internal static readonly string CheckpointPropertyVersion = "version";
        internal static readonly string CheckpointPropertyValid = "valid";
        internal static readonly string CheckpointPropertyOffsetV1 = "offsetV1";
        internal static readonly string CheckpointPropertySequenceNumberV1 = "sequenceNumberV1";
    }
}
