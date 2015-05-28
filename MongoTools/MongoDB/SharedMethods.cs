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
        /// <param name="sourceDatabase"></param>
        /// <param name="targetDatabase"></param>
        /// <param name="buffer"></param>
        /// <param name="sourceCollection"></param>
        /// <param name="insertBatchSize"></param>
        public static void CopyCollection (MongoDatabase sourceDatabase, MongoDatabase targetDatabase, string sourceCollectionName, string targetCollectionName = "", int insertBatchSize = 100, bool copyIndexes = false, bool dropCollections = false)
        {
            var logger = NLog.LogManager.GetLogger ("CopyCollection");
            try
            {                
                // Local Buffer
                List<BsonDocument> buffer = new List<BsonDocument> (insertBatchSize);

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
                    logger.Warn ("Collection not found " + sourceDatabase.Name + "." + sourceCollectionName);
                    return;
                }

                if (sourceCollection.IsCapped ())
                {
                    logger.Warn ("Skiping capped collection " + sourceDatabase.Name + "." + sourceCollectionName);
                    return;    
                }

                // Checking for the need to drop the collection before adding data to it
                if (dropCollections && targetCollection.Exists ())
                {
                    try
                    {
                        targetCollection.Drop ();
                    }
                    catch (Exception ex)
                    {
                        logger.Warn ("Cannot drop collection " + targetCollection.Name, ex);
                        return;
                    }
                }

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
                            if (loop++ % 100 == 1)
                            {
                                logger.Debug ("progress {0}.{1} : {2} ", sourceDatabase.Name, sourceCollection, count);
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
                        logger.Debug ("progress {0}.{1} : {2} ", sourceDatabase.Name, sourceCollection, count);
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
                    logger.Debug ("start index creation {0}.{1} ", sourceDatabase.Name, sourceCollection);
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

                        logger.Debug ("creating index {0}.{1} : {2}", sourceDatabase.Name, sourceCollection, idx.Name);

                        // Adding Index
                        try
                        {
                            targetCollection.CreateIndex (idx.Key, opts);
                        }
                        catch (Exception ex)
                        {
                            logger.Error ("Error creating index " + idx.Name, ex);
                        }
                    }
                    logger.Debug ("index creation completed {0}.{1} ", sourceDatabase.Name, sourceCollection);
                }
            }
            catch (Exception ex)
            {
                logger.Error ("Error copying collection " + sourceCollectionName, ex);
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
                    finalQuery = Query.And (query, Query.GT (indexField, last[indexField]));                    
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
                        logger.Warn (ex.Message + ". try {0} of {1}. Last index: {2}", errorCount, 5, last != null && last.Contains (indexField) ? last[indexField].ToJson () : "");
                        // if 5 consecutive errors... throw
                        if (errorCount++ > 4)
                            throw;                        
                        // else lets wait a while before retrying...
                        System.Threading.Thread.Sleep (2000);
                        break;
                    }
                    
                    // if we got here, return the document
                    yield return last;
                }
            }
        }
    }
}
