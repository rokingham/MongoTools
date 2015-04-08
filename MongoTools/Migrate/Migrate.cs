/*
 *  Parameters:
 *   -h : Shows Help
 *   
 *   -full : Indicates that the whole database should be copied (This is exclusive with -collections and -collections-mask)
 *   
 *   -collections : Indicates that a list of collections to be copied will be received (This is exclusive with -full and -collections-mask)
 *   
 *   -collections-mask : Indicates that a mask will be received to match the collections names that should be copied (This is exclusive with -full and -collections)
 * 
 *   -copy-indexes : Indicates whether the collection indexes must be copied aswell or not (it not received, the indexes won't be copied)
 *   
 *   -drop-collections : If received will force each TARGET collection to be droped prior to being updated. This should be used when you want the target collection to be
 *                       empty before the copy operation kicks in
 * 
 *  Examples of usage:
 *      Copying Full Database                                                 : Migrate.exe -full
 *      Copying Full Database, droping the target before:                     : Migrate.exe -full -drop-collections
 *      Copying Full Database with Indexes                                    : Migrate.exe -full -copy-indexes
 *      Copying a List of Collections (with indexes)                          : Migrate.exe -collections "collection1" "collection2" "awesomeCollection" -copy-indexes
 *      Copying a List of Collections (with indexes), droping each one before : Migrate.exe -collections "collection1" "collection2" "awesomeCollection" -copy-indexes -drop-collections
 *      Copying All Collections that matches the mask "Products_Collection"   : Migrate.exe -collections-mask "Products_Collection"
 * 
 */

using MongoDB;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Migrate
{
    class Migrate
    {
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
        private static int    _insertBatchSize;

        // Arguments
        private static CopyMode           _copyMode;
        private static bool               _copyIndexes;
        private static bool               _dropCollections;
        private static Lazy<List<String>> _collections = new Lazy<List<String>> ();

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

            Console.WriteLine ("Migrating Data");

            // Picking which method to use
            switch (_copyMode)
            {
                case CopyMode.FullDatabaseCopy:
                    Console.WriteLine ("Copying Full Database");
                    Migrator.DatabaseCopy (sourceDatabase, targetDatabase, _insertBatchSize, _copyIndexes, _dropCollections);                    
                break;

                case CopyMode.CollectionsCopy:
                    Console.WriteLine ("Copying Collections from List");
                    Migrator.CollectionsCopy (sourceDatabase, targetDatabase, _collections, _insertBatchSize, _copyIndexes, _dropCollections);                    
                break;

                case CopyMode.CollectionsMaskCopy:
                    Console.WriteLine ("Copying Collections that matches : " + _collections.Value.First());
                    Migrator.CollectionsCopy (sourceDatabase, targetDatabase, _collections.Value.First (), _insertBatchSize, _copyIndexes, _dropCollections);                    
                break;
            }

            Console.WriteLine ("Copy Finished");
            Console.ReadLine ();
        }

        /// <summary>
        /// Loads Configuration from the App.Config file
        /// </summary>
        private static void LoadConfiguration()
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
        /// Prints the Help menu for this Tool
        /// </summary>
        private static void PrintHelp ()
        {
            Console.WriteLine ("***********************************");
            Console.WriteLine ("List of Parameters");
            Console.WriteLine ("\t-h : Shows Help");
            Console.WriteLine ("\t-full : Copies full database");
            Console.WriteLine ("\t-copy-indexes : Indexes will be copied if this is received");
            Console.WriteLine ("\t-collections col1 col2 col3 : If received this will be used instead of full database copy");
            Console.WriteLine ("\t-collections-mask : mask of the collection name");
            Console.WriteLine ("\t-drop-collections: If received, will force drop into each collection before copying the data");
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
                _copyMode = CopyMode.FullDatabaseCopy;
            }
            else // If its not full copy, than, what it is ?
            {
                // Is it Collections or Collections-Mask ? 
                if (args.Where (t => t.Equals (Args.COLLECTIONS_COPY)).FirstOrDefault () != null)
                {
                    _copyMode = CopyMode.CollectionsCopy;
                }
                else if (args.Where (t => t.Equals (Args.COLLECTIONS_MASK)).FirstOrDefault () != null)
                {
                    _copyMode = CopyMode.CollectionsMaskCopy;
                }
                else // If no parameter was set (neither "full", "collections" or "collections-mask", aborts)
                {
                    Console.WriteLine ("No 'copy-parameter' received. Expected either : -full , -collections or -collections-mask");
                    System.Environment.Exit (-102);
                }
            }

            // Checking for index-copy parameter
            _copyIndexes = (args.Where (t => t.Equals (Args.COPY_INDEXES)).FirstOrDefault () != null);

            // Checking for drop-collections parameter
            _dropCollections = (args.Where (t => t.Equals (Args.DROP_COLLECTIONS)).FirstOrDefault () != null);

            // Parsing the rest of the args based on the ones received
            switch (_copyMode)
            {
                // Nothing more should be parsed
                case CopyMode.FullDatabaseCopy:
                break;

                 // Parsing collection names after the argument
                case CopyMode.CollectionsCopy:

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

                case CopyMode.CollectionsMaskCopy:

                    startIndex = GetArgumentIndex (args, Args.COLLECTIONS_MASK);
                    _collections.Value.Add (args[startIndex + 1]);

                break;
            }
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
            for (int i = 0 ; i <= args.Count() ; i++)
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


