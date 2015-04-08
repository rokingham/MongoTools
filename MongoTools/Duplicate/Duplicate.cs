using MongoDB;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Duplicate
{
    class Duplicate
    {
        // Mongo Related Attributes
        private static string _authDatabaseName;
        private static string _sourceServer;
        private static string _sourceUsername;
        private static string _sourcePassword;
        private static string _sourceDatabaseName;
        private static int    _insertBatchSize;

        // Arguments
        private static DuplicateMode      _duplicateMode;
        private static bool               _copyIndexes;
        private static string             _duplicationSuffix;
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
            if (args.Where (t => t == "-h").FirstOrDefault () != null)
            {
                // Printing Help - Parameters
                PrintHelp ();
                return;
            }

            // Reading App Config
            LoadConfiguration ();

            // Parsing Arguments
            ParseArguments (args);

            Console.WriteLine ("Reaching Database");

            // Building Connection Strings
            String sourceConnString      = MongoDbContext.BuildConnectionString (_sourceUsername, _sourcePassword, _sourceServer, _authDatabaseName);

            // Reaching Database
            MongoDatabase sourceDatabase = MongoDbContext.GetServer (sourceConnString).GetDatabase (_sourceDatabaseName);

            // Picking which method to use
            switch (_duplicateMode)
            {
                case DuplicateMode.DuplicateCollections:
                    Console.WriteLine ("Duplicating collection from List");
                    Duplicator.CollectionsDuplicate (sourceDatabase, _collections, _insertBatchSize, _copyIndexes, _duplicationSuffix);
                break;

                case DuplicateMode.DuplicateCollectionsWithMask:
                    Console.WriteLine ("Duplicating Collections that matches : " + _collections.Value.First ());
                    Duplicator.CollectionsDuplicate (sourceDatabase, _collections.Value.First(), _insertBatchSize, _copyIndexes, _duplicationSuffix);
                break;
            }

            Console.WriteLine ("Duplication Finished");
            Console.ReadLine ();
        }

        /// <summary>
        /// Loads Configuration from the App.Config file
        /// </summary>
        private static void LoadConfiguration()
        {
           _authDatabaseName   = ConfigurationManager.AppSettings["authDatabaseName"];
           _sourceServer       = ConfigurationManager.AppSettings["sourceServer"  ];
           _sourceUsername     = ConfigurationManager.AppSettings["sourceUsername"];
           _sourcePassword     = ConfigurationManager.AppSettings["sourcePassword"];
           _sourceDatabaseName = ConfigurationManager.AppSettings["sourceDatabaseName"];
           _insertBatchSize    = Int32.Parse (ConfigurationManager.AppSettings["insertBatchSize"]);
        }

        /// <summary>
        /// Prints the Help menu for this Tool
        /// </summary>
        private static void PrintHelp ()
        {
             Console.WriteLine ("***********************************");
             Console.WriteLine ("List of Parameters");
             Console.WriteLine ("\t-h : Shows Help");
             Console.WriteLine ("\t-copy-indexes : Indexes will be copied if this is received");
             Console.WriteLine ("\t-collections col1 col2 col3 : List of collections to be duplicated");
             Console.WriteLine ("\t-collections-mask : Mask of the name of collections that will be duplicated");
             Console.WriteLine ("\t-duplication-suffix : Suffix that will be appended to the duplicated collection. Default is \"_copy\"");
             Console.WriteLine ("***********************************");
        }

        /// <summary>
        /// Parses out the Arguments received from the "CLI"
        /// </summary>
        /// <param name="args">Array of arguments received from the "CLI"</param>
        private static void ParseArguments (string[] args)
        {
            // Is it Collections or Collections-Mask ? 
            if (args.Where (t => t.Equals (Args.COLLECTIONS_COPY)).FirstOrDefault () != null)
            {
                _duplicateMode = DuplicateMode.DuplicateCollections;
            }
            else if (args.Where (t => t.Equals (Args.COLLECTIONS_MASK)).FirstOrDefault () != null)
            {
                _duplicateMode = DuplicateMode.DuplicateCollectionsWithMask;
            }
            else // If no parameter was set (neither "collections" or "collections-mask", aborts)
            {
                Console.WriteLine ("No 'copy-parameter' received. Expected either : -collections or -collections-mask");
                System.Environment.Exit (-102);
            }

            // Checking for index-copy parameter
            _copyIndexes = (args.Where (t => t.Equals (Args.COPY_INDEXES)).FirstOrDefault () != null);

            // Checking for "DUPLICATION_SUFFIX" parameter
            if (args.Where (t => t.Equals (Args.DUPLICATION_SUFFIX)).FirstOrDefault() != null)
            {
                // Reaching the index of the "Suffix" parameter received
                int suffixIndex = GetArgumentIndex (args, Args.DUPLICATION_SUFFIX) + 1;

                // Reading actual "Suffix" received
                _duplicationSuffix = args[suffixIndex];
            }
            else // If no "Suffix" received, uses the default one
            {
                _duplicationSuffix = "_COPY";
            }
            
            // Parsing the rest of the args based on the ones received
            switch (_duplicateMode)
            {
                // Parsing collection names after the argument
                case DuplicateMode.DuplicateCollections:

                    // Reading arguments for the collection names
                    int startIndex = GetArgumentIndex (args, Args.COLLECTIONS_COPY) + 1;
                    for (int index = startIndex; index < args.Length; index++)
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

                case DuplicateMode.DuplicateCollectionsWithMask:

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
