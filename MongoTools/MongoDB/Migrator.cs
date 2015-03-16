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
        public static void DatabaseCopy (MongoDatabase sourceDatabase, MongoDatabase targetDatabase, int insertBatchSize = 100, bool copyIndexes = true, bool dropCollections = false)
        {
            // Copying All (Non 'System') Collections
            foreach (var collection in sourceDatabase.GetCollectionNames ().ToList ())
            {
                SharedMethods.CopyCollection (sourceDatabase, targetDatabase, collection, insertBatchSize, copyIndexes, dropCollections);
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
            foreach (var collection in collectionsToCopy.Value)
            {
                SharedMethods.CopyCollection (sourceDatabase, targetDatabase, collection, insertBatchSize, copyIndexes, dropCollections);
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
            foreach (var collection in sourceDatabase.GetCollectionNames ().Where (t => t.Contains (collectionsNameMask)))
            {
                SharedMethods.CopyCollection (sourceDatabase, targetDatabase, collection, insertBatchSize, copyIndexes, dropCollections);
            }
        }
    }
}
