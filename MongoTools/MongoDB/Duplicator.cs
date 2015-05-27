using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MongoDB
{
    public class Duplicator
    {
        /// <summary>
        /// Duplicates all the collections whose name is on the List
        /// of collections received.
        /// </summary>
        /// <param name="database">Database name</param>
        /// <param name="collectionsToDuplicate">List of collections to be duplicated</param>
        /// <param name="insertBatchSize">Batch Insert size</param>
        /// <param name="copyIndexes">True if the indexes should be copied, false otherwise</param>
        /// <param name="duplicationSuffix">Suffix that wil be appended to the name of the collection, when duplicated</param>
        public static void CollectionsDuplicate (MongoDatabase database, Lazy<List<String>> collectionsToDuplicate, int insertBatchSize = 100, bool copyIndexes = true, string duplicationSuffix = "_COPY")
        {
            if (String.IsNullOrWhiteSpace (duplicationSuffix))
            {
                throw new ArgumentNullException ("duplicationSuffix");
            }
            // Parallel Options
            var multiThreadingOptions = new ParallelOptions () { MaxDegreeOfParallelism = System.Environment.ProcessorCount * 2};

            // Multi-threading Processing of each duplicate request
            Parallel.ForEach (collectionsToDuplicate.Value, multiThreadingOptions, collectionName =>
            {
                // Console Feedback
                Console.WriteLine ("Duplicating Collection : " + collectionName);

                // Duplication Method
                SharedMethods.CopyCollection (database, database, collectionName, collectionName + duplicationSuffix, insertBatchSize, copyIndexes, true);
            });
        }

        /// <summary>
        /// Duplicates all the collections whose name is on the List
        /// of collections received.
        /// </summary>
        /// <param name="database">Database name</param>
        /// <param name="collectionsNameMask">Mask used to match collections. If a collection name contains this mask, it will be duplicated</param>
        /// <param name="insertBatchSize">Batch Insert size</param>
        /// <param name="copyIndexes">True if the indexes should be copied, false otherwise</param>
        /// <param name="duplicationSuffix">Suffix that wil be appended to the name of the collection, when duplicated</param>
        public static void CollectionsDuplicate (MongoDatabase database, String collectionsNameMask, int insertBatchSize = 100, bool copyIndexes = true, string duplicationSuffix = "_COPY")
        {
            if (String.IsNullOrWhiteSpace (duplicationSuffix))
            {
                throw new ArgumentNullException ("duplicationSuffix");
            }

            // Parallel Options
            var multiThreadingOptions = new ParallelOptions () { MaxDegreeOfParallelism = System.Environment.ProcessorCount * 2};

            // Multi-threading Processing of each duplicate request
            Parallel.ForEach (database.GetCollectionNames ().Where (t => t.Contains (collectionsNameMask)), multiThreadingOptions, collectionName =>
            {
                // Console Feedback
                Console.WriteLine ("Duplicating Collection : " + collectionName);

                // Duplication Method
                SharedMethods.CopyCollection (database, database, collectionName, collectionName + duplicationSuffix, insertBatchSize, copyIndexes, true);
            });
        }
    }
}
