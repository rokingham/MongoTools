﻿using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Builders;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MongoDB
{
    public class SharedMethods
    {
        /// <summary>
        /// Copies a certain collection from one database to the other, including Indexes
        /// </summary>
        /// <param name="olddb"></param>
        /// <param name="newdb"></param>
        /// <param name="buffer"></param>
        /// <param name="col"></param>
        /// <param name="insertBatchSize"></param>
        public static void CopyCollection (MongoDatabase olddb, MongoDatabase newdb, string col, int insertBatchSize, bool copyIndexes, bool dropCollections)
        {
            // Local Buffer
            List<BsonDocument> buffer = new List<BsonDocument> (insertBatchSize);

            // Resets Counter
            int count = 0;

            // Reaching Collections
            var sourceCollection = olddb.GetCollection (col);
            var targetCollection = newdb.GetCollection (col);

            // Checking for the need to drop the collection before adding data to it
            if (dropCollections)
            {
                targetCollection.Drop ();
            }

            IMongoQuery query = null;

            // Skipping System Collections - For Safety Reasons
            if (sourceCollection.FullName.IndexOf ("system.", StringComparison.OrdinalIgnoreCase) >= 0)
            {
                return;
            }

            // Running Copy
            foreach (BsonDocument i in sourceCollection.Find (query).SetSortOrder ("_id"))
            {
                // Feedback and Local Buffer
                count++;

                buffer.Add (i);

                // Dumping data to database every 'X' records
                if (buffer.Count >= insertBatchSize)
                {
                    targetCollection.InsertBatch (buffer);
                    buffer.Clear ();
                    Console.WriteLine ("progress {0}.{1} : {2} ", olddb.Name, col, count);
                }
            }

            // Copying Remaining of Local Buffer
            if (buffer.Count > 0)
            {
                targetCollection.InsertBatch (buffer);
                buffer.Clear ();
                Console.WriteLine ("progress {0}.{1} : {2} ", olddb.Name, col, count);
            }

            // Checkign for the need to copy indexes aswell
            if (copyIndexes)
            {
                // Copying Indexes - If Any
                foreach (IndexInfo idx in sourceCollection.GetIndexes ())
                {
                    // Skipping "_id_" default index - Since Every mongodb Collection has it
                    if (idx.Name == "_id_")
                    {
                        continue;
                    }

                    // Recreating Index Options based on the current index options
                    var opts = IndexOptions.SetBackground (idx.IsBackground)
                                           .SetSparse (idx.IsSparse).SetUnique (idx.IsUnique).SetName (idx.Name).SetDropDups (idx.DroppedDups);

                    if (idx.TimeToLive < TimeSpan.MaxValue)
                    {
                        opts.SetTimeToLive (idx.TimeToLive);
                    }

                    // Adding Index
                    targetCollection.EnsureIndex (idx.Key, opts);
                }
            }
        }

        public static void DuplicateCollection (MongoDatabase database, string collection, string duplicationSuffix, int insertBatchSize ,bool copyIndexes)
        {
            // Local Buffer
            List<BsonDocument> buffer = new List<BsonDocument> (insertBatchSize);

            // Resets Counter
            int count = 0;

            // Reaching Collections (source one and the target one with the suffix)
            var sourceCollection = database.GetCollection (collection);
            var targetCollection = database.GetCollection (collection + duplicationSuffix);

            IMongoQuery query = null;

            // Skipping System Collections - For Safety Reasons
            if (sourceCollection.FullName.IndexOf ("system.", StringComparison.OrdinalIgnoreCase) >= 0)
            {
                return;
            }

            // Running Copy
            foreach (BsonDocument i in sourceCollection.Find (query).SetSortOrder ("_id"))
            {
                // Feedback and Local Buffer
                count++;

                buffer.Add (i);

                // Dumping data to database every 'X' records
                if (buffer.Count >= insertBatchSize)
                {
                    targetCollection.InsertBatch (buffer);
                    buffer.Clear ();
                    Console.WriteLine ("progress {0}.{1} : {2} ", database.Name, collection, count);
                }
            }

            // Copying Remaining of Local Buffer
            if (buffer.Count > 0)
            {
                targetCollection.InsertBatch (buffer);
                buffer.Clear ();
                Console.WriteLine ("progress {0}.{1} : {2} ", database.Name, collection, count);
            }

            // Checkign for the need to copy indexes aswell
            if (copyIndexes)
            {
                // Copying Indexes - If Any
                foreach (IndexInfo idx in sourceCollection.GetIndexes ())
                {
                    // Skipping "_id_" default index - Since Every mongodb Collection has it
                    if (idx.Name == "_id_")
                    {
                        continue;
                    }

                    // Recreating Index Options based on the current index options
                    var opts = IndexOptions.SetBackground (idx.IsBackground)
                                           .SetSparse (idx.IsSparse).SetUnique (idx.IsUnique).SetName (idx.Name).SetDropDups (idx.DroppedDups);

                    if (idx.TimeToLive < TimeSpan.MaxValue)
                    {
                        opts.SetTimeToLive (idx.TimeToLive);
                    }

                    // Adding Index
                    targetCollection.EnsureIndex (idx.Key, opts);
                }
            }
        }
    }
}
