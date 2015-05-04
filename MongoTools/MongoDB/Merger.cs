using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MongoDB
{
    public class Merger
    {
         /// <summary>
        /// Merges all collections from one database into a single target collection
        /// </summary>
        /// <param name="sourceDatabase">Source database - Where the data will come from</param>
        /// <param name="targetDatabase">Target database - Where the data will go to</param>
        /// <param name="targetCollection">Target collection - Where all the data will be merged to</param>
        /// <param name="insertBatchSize">Size (in records) of the chunk of data that will be inserted per batch</param>
        public static void DatabaseMerge (MongoDatabase sourceDatabase, MongoDatabase targetDatabase, string targetCollection, int insertBatchSize = 100)
        {
            // Parallel Options
            var multiThreadingOptions = new ParallelOptions () { MaxDegreeOfParallelism = System.Environment.ProcessorCount * 2 };

            // Multi-threading Processing of each copy request
            foreach (var collectionName in sourceDatabase.GetCollectionNames ())
            {
                // Console Feedback
                Console.WriteLine ("Merging Collection : " + collectionName);

                SharedMethods.CopyCollection (sourceDatabase, targetDatabase, collectionName, targetCollection, insertBatchSize);
            }
        }

        /// <summary>
        /// Merges all the collections received, into a single target collection
        /// </summary>
        /// <param name="sourceDatabase">Source database - Where the data will come from</param>
        /// <param name="targetDatabase">Target database - Where the data will go to</param>
        /// <param name="targetCollection">Target collection - Where all the data will be merged to</param>
        /// <param name="collectionsToCopy">List of names of the collections that should be copied</param>
        /// <param name="insertBatchSize">Size (in records) of the chunk of data that will be inserted per batch</param>
        public static void CollectionsMerge (MongoDatabase sourceDatabase, MongoDatabase targetDatabase, String targetCollection, Lazy<List<String>> collectionsToCopy, int insertBatchSize = 100)
        {
            // Multi-threading Processing of each copy request
            foreach (var collectionName in collectionsToCopy.Value)
            {
                // Console Feedback
                Console.WriteLine ("Merging Collection : " + collectionName);

                SharedMethods.CopyCollection (sourceDatabase, targetDatabase, collectionName, targetCollection, insertBatchSize);
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
        public static void CollectionsMerge (MongoDatabase sourceDatabase, MongoDatabase targetDatabase, String targetCollection, String collectionsNameMask, int insertBatchSize = 100)
        {
             // Parallel Options
            var multiThreadingOptions = new ParallelOptions () { MaxDegreeOfParallelism = System.Environment.ProcessorCount * 2 };

            // Multi-threading Processing of each copy request
            foreach (var collectionName in sourceDatabase.GetCollectionNames ().Where (t => t.Contains (collectionsNameMask)))
            {
                // Console Feedback
                Console.WriteLine ("Merging Collection : " + collectionName);

                SharedMethods.CopyCollection (sourceDatabase, targetDatabase, collectionName, targetCollection, insertBatchSize);
            }
        }
    }
}
