using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Builders;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MongoDB
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
        public static void DatabaseCopy (MongoDatabase sourceDatabase, MongoDatabase targetDatabase, int insertBatchSize = 100, bool copyIndexes = true, bool dropCollections = false, int threads = 1)
        {
            var collections = sourceDatabase.GetCollectionNames ().ToList ();
            if (threads <= 1)
            {
                foreach (var collectionName in collections)
                {
                    SharedMethods.CopyCollection (sourceDatabase, targetDatabase, collectionName, String.Empty, insertBatchSize, copyIndexes, dropCollections);                    
                }
            }
            else
            {
                // Multi-threading Processing of each copy request
                MongoDB.SimpleHelpers.ParallelTasks<string>.Process (collections, 0, threads, collectionName =>
                {
                    // Console Feedback
                    Console.WriteLine ("Migrating Collection : " + collectionName);

                    SharedMethods.CopyCollection (sourceDatabase, targetDatabase, collectionName, String.Empty, insertBatchSize, copyIndexes, dropCollections);
                });
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
        public static void CollectionsCopy (MongoDatabase sourceDatabase, MongoDatabase targetDatabase, List<string> collectionsToCopy, int insertBatchSize = 100, bool copyIndexes = true, bool dropCollections = false, int threads = 1)
        {
            var collections = collectionsToCopy;
            if (threads <= 1)
            {
                foreach (var collectionName in collections)
                    SharedMethods.CopyCollection (sourceDatabase, targetDatabase, collectionName, String.Empty, insertBatchSize, copyIndexes, dropCollections);
            }
            else
            {
                // Multi-threading Processing of each copy request
                MongoDB.SimpleHelpers.ParallelTasks<string>.Process (collections, 0, threads, collectionName =>
                {
                    // Console Feedback
                    Console.WriteLine ("Migrating Collection : " + collectionName);

                    SharedMethods.CopyCollection (sourceDatabase, targetDatabase, collectionName, String.Empty, insertBatchSize, copyIndexes, dropCollections);
                });
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
        public static void CollectionsCopy (MongoDatabase sourceDatabase, MongoDatabase targetDatabase, String collectionsNameMask, int insertBatchSize = 100, bool copyIndexes = true, bool dropCollections = false, int threads = 1)
        {
            var collections = sourceDatabase.GetCollectionNames ().Where (t => t.Contains (collectionsNameMask));
            if (threads <= 1)
            {
                foreach (var collectionName in collections)
                    SharedMethods.CopyCollection (sourceDatabase, targetDatabase, collectionName, String.Empty, insertBatchSize, copyIndexes, dropCollections);
            }
            else
            {
                // Multi-threading Processing of each copy request
                MongoDB.SimpleHelpers.ParallelTasks<string>.Process (collections, 0, threads, collectionName =>
                {
                    // Console Feedback
                    Console.WriteLine ("Migrating Collection : " + collectionName);

                    SharedMethods.CopyCollection (sourceDatabase, targetDatabase, collectionName, String.Empty, insertBatchSize, copyIndexes, dropCollections);
                });
            }
        }
    }
}
