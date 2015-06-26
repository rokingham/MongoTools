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
    public class SharedMethods
    {
        /// <summary>
        /// Copies a certain collection from one database to the other, including Indexes
        /// </summary>
        /// <param name="sourceDatabase"></param>
        /// <param name="targetDatabase"></param>
        /// <param name="buffer"></param>
        /// <param name="sourceCollection"></param>
        /// <param name="insertBatchSize"></param>
        public static void CopyCollection (MongoDatabase sourceDatabase, MongoDatabase targetDatabase, string sourceCollectionName, string targetCollectionName = "", int insertBatchSize = -1, bool copyIndexes = false, bool dropCollections = false)
        {
            var logger = NLog.LogManager.GetLogger ("CopyCollection");
            try
            {   
                // Resets Counter
                long count = 0;
                int loop = 0;

                // Reaching Collections
                var sourceCollection = sourceDatabase.GetCollection (sourceCollectionName);
                var targetCollection = targetDatabase.GetCollection (String.IsNullOrEmpty (targetCollectionName) ? sourceCollectionName : targetCollectionName);

                // Skipping System Collections - For Safety Reasons
                if (sourceCollection.FullName.IndexOf ("system.", StringComparison.OrdinalIgnoreCase) >= 0)
                {
                    return;
                }               

                if (!sourceCollection.Exists ())
                {
                    logger.Warn ("{0}.{1} - Collection not found ", sourceDatabase.Name, sourceCollectionName);
                    return;
                }

                logger.Debug ("{0}.{1} - Start collection copy.", sourceDatabase.Name, sourceCollectionName);

                try
                {
                    // check if collection is capped collection
                    // since this is a special type of collection, we will skip it
                    if (sourceCollection.IsCapped ())
                    {
                        logger.Warn ("{0}.{1} - Skiping capped collection (feature not implemented)", sourceDatabase.Name, sourceCollectionName);
                        return;
                    }

                    // check if batch size is set to auto
                    if (insertBatchSize < 1)
                    {
                        var stats = sourceCollection.GetStats ();
                        // older mongodb vertions < 1.8, has a 4mb limit for batch insert
                        insertBatchSize = ((4 * 1024 * 1024) / (int)stats.AverageObjectSize) + 1;
                        // also benchmarks didn't show any benefit for batches larger than 100...
                        if (insertBatchSize > 200)
                            insertBatchSize = 200;

                        logger.Debug ("{0}.{1} - Insert batch size: {2}", sourceDatabase.Name, sourceCollection.Name, insertBatchSize);
                    }
                }
                catch (Exception ex)
                {
                    logger.Warn (ex, "{0}.{1} - Failed to get collection statistics... continuing any way...", sourceDatabase.Name, sourceCollection.Name);
                }

                // Checking for the need to drop the collection before adding data to it
                if (dropCollections && targetCollection.Exists ())
                {
                    try
                    {
                        targetCollection.Drop ();
                        logger.Debug ("{0}.{1} - Target collection droped: {2}.{3}.", sourceDatabase.Name, sourceCollectionName, targetDatabase.Name, targetCollection.Name);
                    }
                    catch (Exception ex)
                    {
                        logger.Error (ex, "{0}.{1} - Failed to drop target collection {2}.{3}, aborting collection copy...", sourceDatabase.Name, sourceCollectionName, targetDatabase.Name, targetCollection.Name);
                        return;
                    }
                }

                // Local Buffer
                List<BsonDocument> buffer = new List<BsonDocument> (insertBatchSize);

                var total = sourceCollection.Count ();

                // Running Copy
                foreach (BsonDocument i in SafeQuery (sourceCollection, "_id"))
                {
                    // Feedback and Local Buffer
                    count++;

                    buffer.Add (i);

                    // Dumping data to database every 'X' records
                    if (buffer.Count >= insertBatchSize)
                    {
                        try
                        {
                            targetCollection.SafeInsertBatch (buffer, 3, true, true);
                            if (loop++ % 100 == 0)
                            {
                                logger.Debug ("{0}.{1} - batch size: {2}, progress: {3} / {4} ({5}) ", sourceDatabase.Name, sourceCollection.Name, insertBatchSize, count, total, ((double)count / total).ToString ("0.0%"));
                            }
                        }
                        catch (Exception ex)
                        {
                            logger.Error (ex);
                            System.Threading.Thread.Sleep (100);
                        }
                        buffer.Clear ();
                    }
                }

                // Copying Remaining of Local Buffer
                if (buffer.Count > 0)
                {
                    try
                    {
                        targetCollection.SafeInsertBatch (buffer, 3, true, true);
                        logger.Debug ("{0}.{1} - batch size: {2}, progress: {3} / {4} ({5}) ", sourceDatabase.Name, sourceCollection.Name, insertBatchSize, count, total, ((double)count / total).ToString ("0.0%"));
                    }
                    catch (Exception ex)
                    {
                        logger.Error (ex);
                    }
                    buffer.Clear ();
                }

                // Checkign for the need to copy indexes aswell
                if (copyIndexes)
                {
                    logger.Debug ("{0}.{1} - Start index creation", sourceDatabase.Name, sourceCollection.Name);
                    // Copying Indexes - If Any
                    foreach (IndexInfo idx in sourceCollection.GetIndexes ().ToList ())
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

                        logger.Debug ("{0}.{1} - Creating index: {2}", sourceDatabase.Name, sourceCollection, idx.Name);

                        // Adding Index
                        try
                        {
                            targetCollection.CreateIndex (idx.Key, opts);
                        }
                        catch (Exception ex)
                        {
                            // check for timeout exception that may occur if the collection is large...
                            if (ex is System.IO.IOException || ex is System.Net.Sockets.SocketException || (ex.InnerException != null && ex.InnerException is System.Net.Sockets.SocketException))
                            {
                                logger.Warn ("{0}.{1} - Timeout creating index {2}, this may occur in large collections. You should check manually after a while.", sourceDatabase.Name, sourceCollection.Name, idx.Name);
                                // wait for index creation....
                                for (var i = 0; i < 30; i++)
                                {
                                    System.Threading.Thread.Sleep (10000);
                                    try
                                    {
                                        if (targetCollection.IndexExists (idx.Name))
                                            break;
                                    } catch {}
                                }
                            }
                            else
                            {
                                logger.Error (ex, "{0}.{1} - Error creating index {2}" + idx.Name);                                
                            }
                            logger.Warn ("{0}.{1} - Index details: {2}", sourceDatabase.Name, sourceCollection.Name, idx.ToJson ());
                        }                        
                    }
                    logger.Debug ("{0}.{1} - Index creation completed", sourceDatabase.Name, sourceCollection);
                }

                logger.Info ("{0}.{1} - Collection copy completed.", sourceDatabase.Name, sourceCollectionName);
            }
            catch (Exception ex)
            {
                logger.Error (ex, "{0}.{1} - Error copying collection ", sourceDatabase.Name, sourceCollectionName ?? "");
                return;
            }
        }

        private static IEnumerable<BsonDocument> SafeQuery (MongoCollection<BsonDocument> sourceCollection, string indexField = "_id", IMongoQuery query = null)
        {
            var logger = NLog.LogManager.GetLogger ("Query");
            int errorCount = 0;
            bool running = true;
            BsonDocument last = null;
            IEnumerator<BsonDocument> cursor;

            if (String.IsNullOrEmpty (indexField))
                indexField = "_id";

            while (running)
            {
                // prepare query
                IMongoQuery finalQuery = null;
                if (last != null && last.Contains (indexField))
                {
                    finalQuery = query != null ? Query.And (query, Query.GT (indexField, last[indexField])) : Query.GT (indexField, last[indexField]);
                }
                else
                {
                    finalQuery = query;
                }

                // prepare cursor
                cursor = sourceCollection.Find (finalQuery).SetSortOrder (indexField).GetEnumerator ();

                // execute
                while (running)
                {
                    try
                    {
                        if (!cursor.MoveNext ())
                        {
                            running = false;
                            break;                            
                        }
                        last = cursor.Current;
                        errorCount = 0;
                    }
                    catch (Exception ex)
                    {
                        logger.Warn ("{0} - {1}.. try {0} of {1}. Last id: {2}", sourceCollection, ex.Message, errorCount, 5, last != null && last.Contains (indexField) ? last[indexField].ToJson () : "");
                        // if 5 consecutive errors... throw
                        if (errorCount++ > 4)
                            throw;                        
                        // else lets wait a while before retrying...
                        System.Threading.Thread.Sleep (2500);
                        break;
                    }
                    
                    // if we got here, return the document
                    yield return last;
                }
            }
        }

        public static string WildcardToRegex (string pattern, bool anchorOnStart = false)
        {
            if (pattern == null)
                return String.Empty;
            pattern = System.Text.RegularExpressions.Regex.Escape (pattern).Replace (@"\*", ".*").Replace (@"\?", ".");
            return (anchorOnStart ? "^" : "") + pattern + "$";
        }

        public static bool HasWildcard (string pattern)
        {
            if (pattern == null)
                return false;
            return (pattern.IndexOf ('*') > 0 || pattern.IndexOf ('?') > 0);
        }

        public static bool WildcardIsMatch (string pattern, string input, bool ignoreCase = true)
        {
            if (input == pattern) 
                return true;
            if (String.IsNullOrWhiteSpace (pattern) || String.IsNullOrWhiteSpace (input))
                return false;
            if (!HasWildcard (pattern))
                return input.Equals (pattern, ignoreCase? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal);                    
            return System.Text.RegularExpressions.Regex.IsMatch (input, WildcardToRegex(pattern, true), ignoreCase ? System.Text.RegularExpressions.RegexOptions.IgnoreCase : System.Text.RegularExpressions.RegexOptions.None);
        }
    }
}
