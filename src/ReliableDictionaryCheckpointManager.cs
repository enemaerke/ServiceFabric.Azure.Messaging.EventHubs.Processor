using Microsoft.ServiceFabric.Data;
using Microsoft.ServiceFabric.Data.Collections;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Azure.Messaging.EventHubs.ServiceFabricProcessor
{
    class ReliableDictionaryCheckpointManager : ICheckpointManager
    {
        private readonly IReliableStateManager reliableStateManager = null;
        private readonly EventProcessorLogging eventProcessorLogging;
        private IReliableDictionary<string, Dictionary<string, object>> store = null;

        internal ReliableDictionaryCheckpointManager(IReliableStateManager rsm, EventProcessorLogging eventProcessorLogging)
        {
            this.reliableStateManager = rsm;
            this.eventProcessorLogging = eventProcessorLogging;
        }

        /// <summary>
        /// Handling the migration from the previous ServiceFabricProcessor (https://github.com/Azure/azure-sdk-for-net/tree/main/sdk/eventhub/Microsoft.Azure.EventHubs.ServiceFabricProcessor)
        /// that was based on the Microsoft.Azure.EventHubs design to this new version that is based on Azure.Messaging.EventHubs design.
        /// See here: https://github.com/Azure/azure-sdk-for-net/blob/main/sdk/eventhub/README.md
        /// </summary>
        public async Task<bool> AttemptMigration(string partitionId, CancellationToken token)
        {
            if (this.store != null)
            {
                try
                {
                    ConditionalValue<IReliableDictionary<string, Dictionary<string, object>>> oldStore = await
                        this.reliableStateManager
                            .TryGetAsync<IReliableDictionary<string, Dictionary<string, object>>>(Constants.LegacyCheckpointDictionaryName)
                            .ConfigureAwait(false);
                    if (oldStore.HasValue)
                    {
                        // any old checkpoints live in the the reliable dictionary under another state-key but also
                        // in a IReliableDictionary<string, Dictionary<string, object>> so this process is about extracting from the old store,
                        // transforming and adding to the new store and deleting the old store
                        using (ITransaction tx = this.reliableStateManager.CreateTransaction())
                        {
                            ConditionalValue<Dictionary<string, object>> rawCheckpoint = await
                                oldStore.Value.TryGetValueAsync(tx, partitionId, Constants.ReliableDictionaryTimeout, token).ConfigureAwait(false);
                            if (rawCheckpoint.HasValue)
                            {
                                // reading the old value
                                var dictionary = rawCheckpoint.Value;
                                int version = (int)dictionary[Constants.CheckpointPropertyVersion];
                                bool valid = (bool)dictionary[Constants.CheckpointPropertyValid];
                                string offset = (string)dictionary[Constants.CheckpointPropertyOffsetV1];
                                long sequenceNumber = (long)dictionary[Constants.CheckpointPropertySequenceNumberV1];

                                if (valid && long.TryParse(offset, out long newOffset))
                                {
                                    var newCheckpoint = new Checkpoint(newOffset, sequenceNumber);
                                    Dictionary<string, object> putThis = newCheckpoint.ToDictionary();

                                    // add migrated value and remove old in same transaction
                                    await this.store.SetAsync(tx, partitionId, putThis, Constants.ReliableDictionaryTimeout, token)
                                        .ConfigureAwait(false);
                                    await oldStore.Value.TryRemoveAsync(tx, partitionId, Constants.ReliableDictionaryTimeout, token)
                                        .ConfigureAwait(false);
                                    await tx.CommitAsync().ConfigureAwait(false);

                                    eventProcessorLogging.Message(
                                        $"Found offset: {offset} and migrated it to: {newOffset} for partition: {partitionId}");
                                    return true;
                                }

                                eventProcessorLogging.Message(
                                    $"Failed to migrate existing offset: {offset} to a long value, was valid: {valid}, for partition: {partitionId}");
                            }
                        }
                    }
                }
                catch (Exception exc) when (!token.IsCancellationRequested)
                {
                    eventProcessorLogging.Message($"Failed during migrate of previous data. Message: {exc.Message}");
                    throw;
                }
            }

            return false;
        }

        public async Task<bool> CheckpointStoreExistsAsync(CancellationToken cancellationToken)
        {
            ConditionalValue<IReliableDictionary<string, Dictionary<string, object>>> tryStore = await 
                this.reliableStateManager.TryGetAsync<IReliableDictionary<string, Dictionary<string, object>>>(Constants.CheckpointDictionaryName).ConfigureAwait(false);
            eventProcessorLogging?.Message($"CheckpointStoreExistsAsync = {tryStore.HasValue}");
            return tryStore.HasValue;
        }

        public async Task<bool> CreateCheckpointStoreIfNotExistsAsync(CancellationToken cancellationToken)
        {
            // Create or get access to the dictionary.
            this.store = await reliableStateManager.GetOrAddAsync<IReliableDictionary<string, Dictionary<string, object>>>(Constants.CheckpointDictionaryName).ConfigureAwait(false);
            eventProcessorLogging?.Message("CreateCheckpointStoreIfNotExistsAsync OK");
            return true;
        }

        public async Task<Checkpoint> CreateCheckpointIfNotExistsAsync(string partitionId, CancellationToken cancellationToken)
        {
            Checkpoint existingCheckpoint = await GetWithRetry(partitionId, cancellationToken).ConfigureAwait(false);

            if (existingCheckpoint == null)
            {
                existingCheckpoint = new Checkpoint(1);
                await PutWithRetry(partitionId, existingCheckpoint, cancellationToken).ConfigureAwait(false);
            }
            eventProcessorLogging?.Message("CreateCheckpointIfNotExists OK");

            return existingCheckpoint;
        }

        public async Task<Checkpoint> GetCheckpointAsync(string partitionId, CancellationToken cancellationToken)
        {
            return await GetWithRetry(partitionId, cancellationToken).ConfigureAwait(false);
        }

        public async Task UpdateCheckpointAsync(string partitionId, Checkpoint checkpoint, CancellationToken cancellationToken)
        {
            await PutWithRetry(partitionId, checkpoint, cancellationToken).ConfigureAwait(false);
        }

        // Throws on error or if cancelled.
        // Returns null if there is no entry for the given partition.
        private async Task<Checkpoint> GetWithRetry(string partitionId, CancellationToken cancellationToken)
        {
            eventProcessorLogging?.Message($"Getting checkpoint for {partitionId}");

            Checkpoint result = null;
            Exception lastException = null;
            for (int i = 0; i < Constants.RetryCount; i++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                lastException = null;

                try
                {
                    using (ITransaction tx = this.reliableStateManager.CreateTransaction())
                    {
                        ConditionalValue<Dictionary<string, object>> rawCheckpoint = await
                            this.store.TryGetValueAsync(tx, partitionId, Constants.ReliableDictionaryTimeout, cancellationToken).ConfigureAwait(false);

                        await tx.CommitAsync().ConfigureAwait(false);

                        // Success! Save the result, if any, and break out of the retry loop.
                        if (rawCheckpoint.HasValue)
                        {
                            result = Checkpoint.CreateFromDictionary(rawCheckpoint.Value);
                        }
                        else
                        {
                            result = null;
                        }
                        break;
                    }
                }
                catch (TimeoutException e)
                {
                    lastException = e;
                }
            }

            if (lastException != null)
            {
                // Ran out of retries, throw.
                throw new Exception("Ran out of retries creating checkpoint", lastException);
            }

            if (result != null)
            {
                eventProcessorLogging?.Message($"Got checkpoint for {partitionId}: {result.Offset}//{result.SequenceNumber}");
            }
            else
            {
                eventProcessorLogging?.Message($"No checkpoint found for {partitionId}: returning null");
            }

            return result;
        }

        private async Task PutWithRetry(string partitionId, Checkpoint checkpoint, CancellationToken cancellationToken)
        {
            eventProcessorLogging?.Message($"Setting checkpoint for {partitionId}: {checkpoint.Offset}//{checkpoint.SequenceNumber}");

            Exception lastException = null;
            for (int i = 0; i < Constants.RetryCount; i++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                lastException = null;

                Dictionary<string, object> putThis = checkpoint.ToDictionary();

                try
                {
                    using (ITransaction tx = this.reliableStateManager.CreateTransaction())
                    {
                        await this.store.SetAsync(tx, partitionId, putThis, Constants.ReliableDictionaryTimeout, cancellationToken).ConfigureAwait(false);
                        await tx.CommitAsync().ConfigureAwait(false);

                        // Success! Break out of the retry loop.
                        break;
                    }
                }
                catch (TimeoutException e)
                {
                    lastException = e;
                }
            }

            if (lastException != null)
            {
                // Ran out of retries, throw.
                throw new Exception("Ran out of retries creating checkpoint", lastException);
            }

            eventProcessorLogging?.Message($"Set checkpoint for {partitionId} OK");
        }
    }
}
