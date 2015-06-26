using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Builders;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MongoToolsLib
{
    public class Migrator
    {
        class CopyInfo
        {
            public MongoDatabase SourceDatabase{ get; set; }
            public MongoDatabase TargetDatabase{ get; set; }
            public string SourceCollection{ get; set; }
            public string TargetCollection { get; set; }
            public int BatchSize { get; set; }
            public bool CopyIndexes { get; set; } 
            public bool DropCollections { get; set; }
        }

        static IEnumerable<Tuple<MongoDatabase,MongoDatabase>> ListDatabases (MongoServer sourceServer, MongoServer targetServer, List<string> sourceDatabases, List<string> targetDatabases)
        {
            if (sourceDatabases == null)
                yield break;
            if (targetDatabases == null || targetDatabases.Count == 0)
                targetDatabases = null;

            // check if we are on the same server!
            bool sameServer = ServersAreEqual (sourceServer, targetServer);

            for (int i = 0; i < sourceDatabases.Count; i++)
            {
                if (SharedMethods.HasWildcard (sourceDatabases[i]))
                {
                    if (sameServer)
                        throw new Exception ("Source and target servers are the same!");
                    foreach (var db in sourceServer.GetDatabaseNames ().Where (name => SharedMethods.WildcardIsMatch (sourceDatabases[i], name, true)))
                    {
                        yield return Tuple.Create (sourceServer.GetDatabase (db), targetServer.GetDatabase (db));                            
                    }
                }
                else if (targetDatabases == null)
                {
                    if (sameServer)
                        throw new Exception ("Source and target servers are the same!");
                    yield return Tuple.Create (sourceServer.GetDatabase (sourceDatabases[i]), targetServer.GetDatabase (sourceDatabases[i]));
                }
                else
                {
                    yield return Tuple.Create (sourceServer.GetDatabase (sourceDatabases[i]), targetServer.GetDatabase (targetDatabases[i]));
                }
            }
        }
 
        private static bool ServersAreEqual (MongoServer sourceServer, MongoServer targetServer)
        {
            // check if we are on the same server!            
            try
            {
                // check using get ip endpoint of the primary server and port
                if (sourceServer.Primary != null && targetServer.Primary != null)
                    return sourceServer.Primary.GetIPEndPoint ().Address.ToString () == targetServer.Primary.GetIPEndPoint ().Address.ToString () &&
                        sourceServer.Primary.Address.Port == targetServer.Primary.Address.Port;
            }
            catch {}

            try
            {
                // fallback to comparing the host names and port
                if (sourceServer.Instance != null && targetServer.Instance != null)
                    return sourceServer.Instance.Address.Host.Equals (targetServer.Instance.Address.Host, StringComparison.OrdinalIgnoreCase) &&
                           sourceServer.Instance.Address.Port == targetServer.Instance.Address.Port;
            }
            catch {}

            return false;
        }

        static IEnumerable<string> ListCollections (MongoDatabase sourceServer, List<string> collections)
        {
            var list = sourceServer.GetCollectionNames ();
            if (collections != null && collections.Count > 0)
            {
                list = list.Where (c => collections.Any (pattern => SharedMethods.WildcardIsMatch (pattern, c, true)));
            }
            return list.ToList ();               
        }

        /// <summary>
        /// Migrates data and indexes of all collections of a certain database, to another
        /// </summary>
        /// <param name="sourceServer">Source mongodb server  - Where the data will come from.</param>
        /// <param name="targetServer">Target mongodb server - Where the data will go to.</param>
        /// <param name="sourceDatabases">The source databases.</param>
        /// <param name="targetDatabases">The target databases.</param>
        /// <param name="collections">The collections.</param>
        /// <param name="insertBatchSize">Size (in records) of the chunk of data that will be inserted per batch.</param>
        /// <param name="copyIndexes">True if the indexes should be copied aswell, false otherwise.</param>
        /// <param name="dropCollections">The drop collections.</param>
        /// <param name="threads">The threads.</param>
        public static void DatabaseCopy (MongoServer sourceServer, MongoServer targetServer, List<string> sourceDatabases, List<string> targetDatabases, List<string> collections, int insertBatchSize = -1, bool copyIndexes = true, bool dropCollections = false, int threads = 1)
        {
            if (threads <= 1)
                threads = 1;

            using (var mgr = new MongoToolsLib.SimpleHelpers.ParallelTasks<CopyInfo> (0, threads, 1000, CollectionCopy))
            {
                // list databases
                foreach (var db in ListDatabases(sourceServer, targetServer, sourceDatabases, targetDatabases))
                {
                    foreach (var col in ListCollections (db.Item1, collections))
                    {
                        // process task
                        mgr.AddTask (new CopyInfo
                        {
                            SourceDatabase   = db.Item1,
                            TargetDatabase   = db.Item2,
                            SourceCollection = col,
                            TargetCollection = col,
                            BatchSize        = insertBatchSize,
                            CopyIndexes      = copyIndexes,
                            DropCollections  = dropCollections
                        });
                    }                    
                }
                mgr.CloseAndWait ();
            }
        }

        static void CollectionCopy (CopyInfo item)
        {
            SharedMethods.CopyCollection (item.SourceDatabase, item.TargetDatabase, item.SourceCollection, item.TargetCollection, item.BatchSize, item.CopyIndexes, item.DropCollections);
        }

        /// <summary>
        /// Migrates data and indexes of all collections of a certain database, to another
        /// </summary>
        /// <param name="sourceDatabase">Source database - Where the data will come from</param>
        /// <param name="targetDatabase">Target database - Where the data will go to</param>
        /// <param name="insertBatchSize">Size (in records) of the chunk of data that will be inserted per batch</param>
        /// <param name="copyIndexes">True if the indexes should be copied aswell, false otherwise</param>
        public static void DatabaseCopy (MongoDatabase sourceDatabase, MongoDatabase targetDatabase, int insertBatchSize = -1, bool copyIndexes = true, bool dropCollections = false, int threads = 1)
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
                MongoToolsLib.SimpleHelpers.ParallelTasks<string>.Process (collections, 0, threads, collectionName =>
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
        public static void CollectionsCopy (MongoDatabase sourceDatabase, MongoDatabase targetDatabase, List<string> collectionsToCopy, int insertBatchSize = -1, bool copyIndexes = true, bool dropCollections = false, int threads = 1)
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
                MongoToolsLib.SimpleHelpers.ParallelTasks<string>.Process (collections, 0, threads, collectionName =>
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
        public static void CollectionsCopy (MongoDatabase sourceDatabase, MongoDatabase targetDatabase, String collectionsNameMask, int insertBatchSize = -1, bool copyIndexes = true, bool dropCollections = false, int threads = 1)
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
                MongoToolsLib.SimpleHelpers.ParallelTasks<string>.Process (collections, 0, threads, collectionName =>
                {
                    // Console Feedback
                    Console.WriteLine ("Migrating Collection : " + collectionName);

                    SharedMethods.CopyCollection (sourceDatabase, targetDatabase, collectionName, String.Empty, insertBatchSize, copyIndexes, dropCollections);
                });
            }
        }
    }
}
