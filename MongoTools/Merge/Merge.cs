/*
 *  Parameters:
 *   -h : Shows Help
 *   
 *   -full : Indicates that the whole database (all collections it contains) should be merged into a single collection (This is exclusive with -collections and -collections-mask)
 *   
 *   -collections : Indicates that a list of collections to be merged will be received (This is exclusive with -full and -collections-mask)
 *   
 *   -collections-mask : Indicates that a mask will be received to match the collections names that should be merged (This is exclusive with -full and -collections)
 * 
 *   -target : The name of the collection where all the data will be merged into. This parameter is mandatory
 *    
 * 
 *  Examples of usage:
 *      Merging Full Database                                                 : Merge.exe -full
 *      Merging a List of Collections                                         : Merge.exe -collections "collection1" "collection2" "awesomeCollection" -target "mergedCollection"
 *      Merging All Collections that matches the mask "Products_Collection"   : Merge.exe -collections-mask "Products_Collection" -target "myMergedCollection"
 * 
 */

using MongoToolsLib;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Merge
{
    class Merge
    {
        #region ** Attributes **

        // Mongo Related Attributes
        private static string _authDatabaseNameSource;
        private static string _authDatabaseNameTarget;
        private static string _sourceServer;
        private static string _sourceUsername;
        private static string _sourcePassword;
        private static string _targetServer;
        private static string _targetUsername;
        private static string _targetPassword;
        private static string _targetDatabaseName;
        private static string _sourceDatabaseName;
        private static string _targetCollectionName;
        private static int    _insertBatchSize;

        // Arguments
        private static MergeMode          _mergeMode;
        private static Lazy<List<String>> _collections = new Lazy<List<String>> ();

        #endregion

        static void Main (string[] args)
        {
             // Parameters Check
            if (args == null || args.Length == 0)
            {
                // Error
                Console.WriteLine ("No arguments received. Type pass '-h' to receive a list of parameters");
                System.Environment.Exit (-101);
            }

            // Should print help ?
            if (args.Where (t => t == "-h").FirstOrDefault() != null)
            {
                // Printing Help - Parameters
                PrintHelp ();
                return;
            }

            // Parsing Arguments - Sanity Check
            ParseArguments (args);

            // Reading App Config
            LoadConfiguration ();
            
            Console.WriteLine ("Reaching Databases");

            // Building Connection Strings
            String sourceConnString = MongoDbContext.BuildConnectionString (_sourceUsername, _sourcePassword, _sourceServer, _authDatabaseNameSource);
            String targetConnString = MongoDbContext.BuildConnectionString (_targetUsername, _targetPassword, _targetServer, _authDatabaseNameTarget);

            // Reaching Databases
            MongoDatabase sourceDatabase = MongoDbContext.GetServer (sourceConnString).GetDatabase (_sourceDatabaseName);
            MongoDatabase targetDatabase = MongoDbContext.GetServer (targetConnString).GetDatabase (_targetDatabaseName);

            Console.WriteLine ("Merging Data");

            // Picking which method to use
            switch (_mergeMode)
            {
                case MergeMode.FullDatabaseMerge:
                    Console.WriteLine ("Merging Full Database into Collection : " + _targetCollectionName);
                    Merger.DatabaseMerge (sourceDatabase, targetDatabase, _targetCollectionName, _insertBatchSize);
                    break;

                case MergeMode.CollectionsMerge:
                    Console.WriteLine ("Merging Collections from List, into Collection : " + _targetCollectionName);
                    Merger.CollectionsMerge (sourceDatabase, targetDatabase, _targetCollectionName, _collections, _insertBatchSize);
                    break;

                case MergeMode.CollectionsMaskMerge:
                    Console.WriteLine ("Merging Collections that matches : " + _collections.Value.First () + " into Collection" + _targetCollectionName);
                    Merger.CollectionsMerge (sourceDatabase, targetDatabase, _targetCollectionName, _collections.Value.First (), _insertBatchSize);
                    break;
            }

            Console.WriteLine ("Merge Finished");
            Console.ReadLine ();
        }

        /// <summary>
        /// Prints the Help menu for this Tool
        /// </summary>
        private static void PrintHelp ()
        {
            Console.WriteLine ("***********************************");
            Console.WriteLine ("List of Parameters");
            Console.WriteLine ("\t-h : Shows Help");
            Console.WriteLine ("\t-full : Merges all collections from the database, to the target one");
            Console.WriteLine ("\t-collections col1 col2 col3 : If received this will be used instead of full database copy");
            Console.WriteLine ("\t-collections-mask : mask of the collection names that should be merged into the target one");
            Console.WriteLine ("\t-target : Name of the collection where the data will be merged to");
            Console.WriteLine ("***********************************");
        }

        /// <summary>
        /// Parses out the Arguments received from the "CLI"
        /// </summary>
        /// <param name="args">Array of arguments received from the "CLI"</param>
        private static void ParseArguments (string[] args)
        {
            // Checking whether the Args.FULL_COPY parameter was received, with no other "collection" parameter set to true
            if ((args.Where (t => t.Equals (Args.FULL_COPY)).FirstOrDefault () != null)
                         && ((args.Where (t => t.Contains (Args.COLLECTIONS_COPY)).FirstOrDefault () == null)))
            {
                _mergeMode = MergeMode.FullDatabaseMerge;
            }
            else // If its not full copy, than, what it is ?
            {
                // Is it Collections or Collections-Mask ? 
                if (args.Where (t => t.Equals (Args.COLLECTIONS_COPY)).FirstOrDefault () != null)
                {
                    _mergeMode = MergeMode.CollectionsMerge;
                }
                else if (args.Where (t => t.Equals (Args.COLLECTIONS_MASK)).FirstOrDefault () != null)
                {
                    _mergeMode = MergeMode.CollectionsMaskMerge;
                }
                else // If no parameter was set (neither "full", "collections" or "collections-mask", aborts)
                {
                    Console.WriteLine ("No 'copy-parameter' received. Expected either : -full , -collections or -collections-mask");
                    System.Environment.Exit (-102);
                }
            }

            // Parsing the rest of the args based on the ones received
            switch (_mergeMode)
            {
                // Nothing more should be parsed
                case MergeMode.FullDatabaseMerge:
                break;

                 // Parsing collection names after the argument
                case MergeMode.CollectionsMerge:

                    // Reading arguments for the collection names
                    int startIndex = GetArgumentIndex (args, Args.COLLECTIONS_COPY) + 1;
                    for (int index = startIndex ; index < args.Length ; index++)
                    {
                        // Checking whether this argument starts with '-' meaning that it is a parameter, and should not be added to the list of collections
                        if (args[index].StartsWith ("-"))
                        {
                            break;
                        }

                        // Adds it to the list of collection names
                        _collections.Value.Add (args[index]);
                    }

                break;

                case MergeMode.CollectionsMaskMerge:

                    startIndex = GetArgumentIndex (args, Args.COLLECTIONS_MASK);
                    _collections.Value.Add (args[startIndex + 1]);

                break;
            }

            // Have we received any TargetCollection parameter ?
            if (args.Any (t => t.Equals (Args.TARGET_COLLECTION)))
            {
                int startIndex        = GetArgumentIndex (args, Args.TARGET_COLLECTION);
                _targetCollectionName = args[startIndex + 1];
            }
            else
            {
                // Abort. This parameter is mandatory
                Console.WriteLine ("No 'target' received. This parameter is mandatory since it indicates where the merging will take place.");
                System.Environment.Exit (-103);
            }
        }

        /// <summary>
        /// Loads Configuration from the App.Config file
        /// </summary>
        private static void LoadConfiguration ()
        {
           _authDatabaseNameSource = ConfigurationManager.AppSettings["authDatabaseNameSource"];
           _authDatabaseNameTarget = ConfigurationManager.AppSettings["authDatabaseNameTarget"];
           _sourceServer           = ConfigurationManager.AppSettings["sourceServer"  ];
           _sourceUsername         = ConfigurationManager.AppSettings["sourceUsername"];
           _sourcePassword         = ConfigurationManager.AppSettings["sourcePassword"];
           _targetServer           = ConfigurationManager.AppSettings["targetServer"  ];
           _targetUsername         = ConfigurationManager.AppSettings["targetUsername"];
           _targetPassword         = ConfigurationManager.AppSettings["targetPassword"];
           _targetDatabaseName     = ConfigurationManager.AppSettings["targetDatabaseName"];
           _sourceDatabaseName     = ConfigurationManager.AppSettings["sourceDatabaseName"];
           _insertBatchSize        = Int32.Parse (ConfigurationManager.AppSettings["insertBatchSize"]);
        }

        /// <summary>
        /// Gets the Index of the received argument (by key) within
        /// the array of arguments
        /// </summary>
        /// <param name="args">Array of Arguments</param>
        /// <param name="argName">Key (name) of the argument searched</param>
        /// <returns>Index of the argument within array. -1 if not found</returns>
        private static int GetArgumentIndex (string[] args, string argName)
        {
            for (int i = 0; i <= args.Count (); i++)
            {
                if (args[i].Equals (argName))
                {
                    return i;
                }
            }

            // Not Found
            return -1;
        }
    }
}
