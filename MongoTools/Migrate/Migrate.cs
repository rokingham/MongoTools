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
using MongoDB.SimpleHelpers;
using NLog;
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
        #region ** Attributes **

        // Mongo Related Attributes
        private static string _sourceAuthDatabase;
        private static string _targetAuthDatabase;
        private static string _sourceServer;
        private static string _sourceUsername;
        private static string _sourcePassword;
        private static string _targetServer;
        private static string _targetUsername;
        private static string _targetPassword;
        private static string _targetDatabaseName;
        private static string _sourceDatabaseName;
        private static int    _insertBatchSize;
        private static int    _threads;

        // Arguments
        private static CopyMode           _copyMode;
        private static bool               _copyIndexes;
        private static bool               _dropCollections;
        private static List<String> _collections = new List<String> ();

        #endregion

        static void Main (string[] args)
        {
            // set error exit code
            System.Environment.ExitCode = -50;
            try
            {
                // load configurations
                ProgramOptions = ConsoleUtils.Initialize (args, true);           
                    
                // start execution
                Execute (ProgramOptions);

                // check before ending for waitForKeyBeforeExit option
                if (ProgramOptions.Get ("waitForKeyBeforeExit", false))
                    ConsoleUtils.WaitForAnyKey ();
            }
            catch (Exception ex)
            {
                LogManager.GetCurrentClassLogger ().Fatal (ex);

                // check before ending for waitForKeyBeforeExit option
                if (ProgramOptions.Get ("waitForKeyBeforeExit", false))
                    ConsoleUtils.WaitForAnyKey ();

                ConsoleUtils.CloseApplication (-60, true);
            }
            // set success exit code
            ConsoleUtils.CloseApplication (0, false);
        }

        static FlexibleOptions ProgramOptions { get; set; }
        static Logger logger = LogManager.GetCurrentClassLogger ();
        static DateTime Started = DateTime.UtcNow;

        private static void Execute (FlexibleOptions options)
        {
            logger.Debug ("Start");
        
            // Parsing Arguments - Sanity Check
            ParseArguments (options);

            // Reading App Config
            LoadConfiguration ();

            logger.Debug ("Opening connections...");

            // Building Connection Strings
            String sourceConnString = MongoDbContext.BuildConnectionString (_sourceUsername, _sourcePassword, true, true, _sourceServer, 30000, 4 * 60000, _sourceAuthDatabase);
            String targetConnString = MongoDbContext.BuildConnectionString (_targetUsername, _targetPassword, true, true, _targetServer, 30000, 4 * 60000, _targetAuthDatabase);

            // Reaching Databases
            MongoDatabase sourceDatabase = MongoDbContext.GetServer (sourceConnString).GetDatabase (_sourceDatabaseName);
            MongoDatabase targetDatabase = MongoDbContext.GetServer (targetConnString).GetDatabase (_targetDatabaseName);

            logger.Debug ("Start migrating data...");
                 
            // Picking which method to use
            switch (_copyMode)
            {
                case CopyMode.FullDatabaseCopy:
                    logger.Debug  ("Copying Full Database");
                    Migrator.DatabaseCopy (sourceDatabase, targetDatabase, _insertBatchSize, _copyIndexes, _dropCollections, _threads);                    
                break;

                case CopyMode.CollectionsCopy:
                    logger.Debug ("Copying Collections from List");
                    Migrator.CollectionsCopy (sourceDatabase, targetDatabase, _collections, _insertBatchSize, _copyIndexes, _dropCollections, _threads); 
                break;

                case CopyMode.CollectionsMaskCopy:
                    logger.Debug ("Copying Collections that matches : " + String.Join (", ", _collections));
                    Migrator.CollectionsCopy (sourceDatabase, targetDatabase, _collections.First (), _insertBatchSize, _copyIndexes, _dropCollections, _threads);                    
                break;
            }

            logger.Debug ("Done migrating data!");
        }

        /// <summary>
        /// Loads Configuration from the App.Config file
        /// </summary>
        private static void LoadConfiguration()
        {
            _sourceAuthDatabase = ProgramOptions.Get ("sourceAuthDatabase", ProgramOptions["authDatabaseNameSource"]);
            _targetAuthDatabase = ProgramOptions.Get ("targetAuthDatabase", ProgramOptions["authDatabaseNameTarget"]);
           _sourceServer            = ProgramOptions["sourceServer"  ];
           _sourceUsername          = ProgramOptions["sourceUsername"];
           _sourcePassword          = ProgramOptions["sourcePassword"];
           _sourceDatabaseName      = ProgramOptions.Get ("sourceDatabase", ProgramOptions["sourceDatabaseName"]);
           _targetServer            = ProgramOptions["targetServer"  ];
           _targetUsername          = ProgramOptions["targetUsername"];
           _targetPassword          = ProgramOptions["targetPassword"];
           _targetDatabaseName      = ProgramOptions.Get ("targetDatabase", ProgramOptions.Get ("targetDatabaseName", ""));
            if (String.IsNullOrEmpty (_targetDatabaseName))
                _targetDatabaseName = _sourceDatabaseName;
           _insertBatchSize         = ProgramOptions.Get ("insertBatchSize", 150);
           _threads                 = ProgramOptions.Get ("threads", 1);

           _copyIndexes             = ProgramOptions.Get ("copy-indexes", true);
           _dropCollections         = ProgramOptions.Get ("drop-collections", false);
        }
        
        /// <summary>
        /// Parses out the Arguments received from the "CLI"
        /// </summary>
        /// <param name="args">Array of arguments received from the "CLI"</param>
        private static void ParseArguments (FlexibleOptions options)
        {
            LoadConfiguration ();

            // Checking whether the Args.FULL_COPY parameter was received, with no other "collection" parameter set to true
            if (ProgramOptions.Get (Args.FULL_COPY, true))
            {
                _copyMode = CopyMode.FullDatabaseCopy;
            }
            else if (ProgramOptions.HasOption (Args.COLLECTIONS_COPY))
            {
                _copyMode = CopyMode.CollectionsCopy;
            }
            else if (ProgramOptions.HasOption (Args.COLLECTIONS_MASK))
            {
                _copyMode = CopyMode.CollectionsMaskCopy;
            }
            else // If no parameter was set (neither "full", "collections" or "collections-mask", aborts)
            {
                logger.Error ("No 'copy-parameter' received. Expected either : -full , -collections or -collections-mask");
                ConsoleUtils.CloseApplication (-102, true);
            }

            // Parsing the rest of the args based on the ones received
            switch (_copyMode)
            {
                // Nothing more should be parsed
                case CopyMode.FullDatabaseCopy:
                break;

                 // Parsing collection names after the argument
                case CopyMode.CollectionsCopy:
                    {
                        var list = ProgramOptions.Get<string[]> (Args.COLLECTIONS_COPY, null);
                        if (list == null)
                            list = ProgramOptions.Get (Args.COLLECTIONS_COPY, "").Split (',', ';');
                        if (list != null)
                        {
                            _collections = list.Select (i => i.Trim ()).Where (i => !String.IsNullOrEmpty (i)).ToList ();
                        }
                    }
                break;

                case CopyMode.CollectionsMaskCopy:
                    {
                        var list = ProgramOptions.Get<string[]> (Args.COLLECTIONS_MASK, null);
                        if (list == null)
                            list = ProgramOptions.Get (Args.COLLECTIONS_MASK, "").Split (',', ';');
                        if (list != null)
                        {
                            _collections = list.Select (i => i.Trim ()).Where (i => !String.IsNullOrEmpty (i)).ToList ();
                        }
                    }
                break;
            }
        }
        
    }
}


