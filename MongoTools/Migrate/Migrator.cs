using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Builders;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Migrate
{
    public class Migrator
    {
        /// <summary>
        /// Migrates data and indexes of all collections of a certain database, to another
        /// </summary>
        /// <param name="sourceDatabase">Source database - Where the data will come from</param>
        /// <param name="targetDatabase">Target database - Where the data will go to</param>
        /// <param name="insertBatchSize">Size (in records) of the chunk of data that will be inserted per batch</param>
        /// <param name="copyIndexes">True if the indexes should be copied aswell, false otherwise</param>
        public static void DatabaseCopy (MongoDatabase sourceDatabase, MongoDatabase targetDatabase, int insertBatchSize = 100, bool copyIndexes = true, bool dropCollections = false)
        {            
            // Copying All (Non 'System') Collections
            foreach (var col in sourceDatabase.GetCollectionNames ().ToList ())
            {
                CopyCollection (sourceDatabase, targetDatabase, col, insertBatchSize, copyIndexes, dropCollections);
            }
        }
        
        /// <summary>
        /// Migrates data and indexes of all collections of a certain database, to another
        /// </summary>
        /// <param name="sourceDatabase">Source database - Where the data will come from</param>
        /// <param name="targetDatabase">Target database - Where the data will go to</param>
        /// <param name="collectionsToCopy">List of names of the collections that should be copied</param>
        /// <param name="insertBatchSize">Size (in records) of the chunk of data that will be inserted per batch</param>
        /// <param name="copyIndexes">True if the indexes should be copied aswell, false otherwise</param>
        public static void CollectionsCopy (MongoDatabase sourceDatabase, MongoDatabase targetDatabase, Lazy<List<String>> collectionsToCopy, int insertBatchSize = 100, bool copyIndexes = true, bool dropCollections = false)
        {
            // Copying All Collections Received as parameter
            foreach (var col in collectionsToCopy.Value)
            {
                CopyCollection (sourceDatabase, targetDatabase, col, insertBatchSize, copyIndexes, dropCollections);
            }
        }

        /// <summary>
        /// Migrates data and indexes of all collections of a certain database, to another
        /// </summary>
        /// <param name="sourceDatabase">Source database - Where the data will come from</param>
        /// <param name="targetDatabase">Target database - Where the data will go to</param>
        /// <param name="collectionsNameMask">Mask that will be used to decide whether one collection will be copied or not - Case Sensitive</param>
        /// <param name="insertBatchSize">Size (in records) of the chunk of data that will be inserted per batch</param>
        /// <param name="copyIndexes">True if the indexes should be copied aswell, false otherwise</param>
        public static void CollectionsCopy (MongoDatabase sourceDatabase, MongoDatabase targetDatabase, String collectionsNameMask, int insertBatchSize = 100, bool copyIndexes = true, bool dropCollections = false)
        {
            // Copying All Collections Received as parameter
            foreach (var col in sourceDatabase.GetCollectionNames().Where (t => t.Contains (collectionsNameMask)))
            {
                CopyCollection (sourceDatabase, targetDatabase, col, insertBatchSize, copyIndexes, dropCollections);
            }
        }

        /// <summary>
        /// Copies a certain collection from one database to the other, including Indexes
        /// </summary>
        /// <param name="olddb"></param>
        /// <param name="newdb"></param>
        /// <param name="buffer"></param>
        /// <param name="col"></param>
        /// <param name="insertBatchSize"></param>
        private static void CopyCollection (MongoDatabase olddb, MongoDatabase newdb, string col, int insertBatchSize, bool copyIndexes, bool dropCollections)
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
    }

    public enum CopyMode
    {
        FullDatabaseCopy, CollectionsCopy, CollectionsMaskCopy
    }
}
