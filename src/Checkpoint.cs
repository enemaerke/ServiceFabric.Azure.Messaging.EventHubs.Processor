using System;
using System.Collections.Generic;

namespace Azure.Messaging.EventHubs.ServiceFabricProcessor
{
    /// <summary>
    /// A persistable representation of what events in the stream have been processed.
    /// Version 1 checkpoint is just a high-water mark, containing an offset and sequence number. All events at or lower than the given position
    /// have been processed. Any events higher than the given position are unprocessed.
    /// </summary>
    public class Checkpoint
    {
        /// <summary>
        /// Create an uninitialized checkpoint of the given version.
        /// </summary>
        /// <param name="version"></param>
        internal Checkpoint(int version)
        {
            this.Version = version;
            this.Valid = false;
        }

        /// <summary>
        /// Create an initialized version 1 checkpoint.
        /// </summary>
        /// <param name="offset">Offset of highest-processed position.</param>
        /// <param name="sequenceNumber">Sequence number of highest-processed position.</param>
        public Checkpoint(long offset, long sequenceNumber)
        {
            this.Version = 1;
            this.Offset = offset;
            this.SequenceNumber = sequenceNumber;
            this.Valid = true;
        }

        #region AllVersions
        //
        // Methods and properties valid for all versions.
        //

        /// <summary>
        /// Version of this checkpoint.
        /// </summary>
        public int Version { get; protected set; }

        /// <summary>
        /// True if this checkpoint contains a valid position.
        /// </summary>
        public bool Valid { get; protected set; }

        /// <summary>
        /// Serialize this instance to a persistable representation as a name-value dictionary.
        /// </summary>
        /// <returns>Serialized dictionary representation.</returns>
        public Dictionary<string, object> ToDictionary()
        {
            Dictionary<string, object> converted = new Dictionary<string, object>();

            converted.Add(Constants.CheckpointPropertyVersion, this.Version);
            converted.Add(Constants.CheckpointPropertyValid, this.Valid);

            switch (this.Version)
            {
                case 1:
                    converted.Add(Constants.CheckpointPropertyOffsetV1, this.Offset);
                    converted.Add(Constants.CheckpointPropertySequenceNumberV1, this.SequenceNumber);
                    break;

                default:
                    throw new NotImplementedException();
            }

            return converted;
        }

        /// <summary>
        /// Deserialize from a name-value dictionary.
        /// </summary>
        /// <param name="dictionary">Serialized representation.</param>
        /// <returns>Deserialized instance.</returns>
        public static Checkpoint CreateFromDictionary(Dictionary<string, object> dictionary)
        {
            int version = (int)dictionary[Constants.CheckpointPropertyVersion];
            bool valid = (bool)dictionary[Constants.CheckpointPropertyValid];

            Checkpoint result = new Checkpoint(version);

            if (valid)
            {
                result.Valid = true;

                switch (result.Version)
                {
                    case 1:
                        result.Offset = (long)dictionary[Constants.CheckpointPropertyOffsetV1];
                        result.SequenceNumber = (long)dictionary[Constants.CheckpointPropertySequenceNumberV1];
                        break;

                    default:
                        throw new NotImplementedException($"Unrecognized checkpoint version {result.Version}");
                }
            }

            return result;
        }
        #endregion AllVersions

        #region Version1
        /// <summary>
        /// Offset of highest-processed position. Immutable after construction or initialization.
        /// </summary>
        public long Offset { get; private set; }

        /// <summary>
        /// Sequence number of highest-processed position. Immutable after construction or initialization.
        /// </summary>
        public long SequenceNumber { get; private set; }
        #endregion Version1
    }
}
