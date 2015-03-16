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
            // Iterating over name of the collections received
            foreach (var collectionName in collectionsToDuplicate.Value)
            {
                SharedMethods.DuplicateCollection (database, collectionName, duplicationSuffix, insertBatchSize, copyIndexes);
            }
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
            foreach (var collection in database.GetCollectionNames ().Where (t => t.Contains (collectionsNameMask)))
            {
                SharedMethods.DuplicateCollection (database, collection, duplicationSuffix, insertBatchSize, copyIndexes);
            }
        }
    }
}
