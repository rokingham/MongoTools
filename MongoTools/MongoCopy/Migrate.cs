using MongoToolsLib;
using MongoDB.Driver;
using MongoToolsLib.SimpleHelpers;
using NLog;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MongoCopy
{
    class Migrate
    {
        #region ** Attributes **

        // Mongo Related Attributes
        private static string _sourceAuthDatabase;
        private static string _targetAuthDatabase;
        private static string _sourceUri;
        private static string _targetUri;
        private static string _sourceServer;
        private static string _sourceUsername;
        private static string _sourcePassword;
        private static string _targetServer;
        private static string _targetUsername;
        private static string _targetPassword;        
        private static int    _insertBatchSize;
        private static int    _threads;

        // Arguments
        private static bool               _copyIndexes;
        private static bool               _dropCollections;
        private static List<String> _collections = new List<String> ();
        private static List<String> _sourceDatabases = new List<String> ();
        private static List<String> _targetDatabases = new List<String> ();        

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

            logger.Debug ("Opening connections...");

            CheckConnections (options);            

            // Reaching Databases
            MongoServer sourceDatabase = MongoDbContext.GetServer (options.Get ("source"));
            MongoServer targetDatabase = MongoDbContext.GetServer (options.Get ("target"));

            // process list
            logger.Debug ("Start migrating data...");

            Migrator.DatabaseCopy (sourceDatabase, targetDatabase, _sourceDatabases, _targetDatabases, _collections, _insertBatchSize, _copyIndexes, _dropCollections, _threads);            

            logger.Debug ("Done migrating data!");
        }        
        
        /// <summary>
        /// Parses out the Arguments received from the "CLI"
        /// </summary>
        /// <param name="args">Array of arguments received from the "CLI"</param>
        private static void ParseArguments (FlexibleOptions options)
        {
            // parse arguments
            _sourceUri          = options["source"];
            _sourceServer       = options["sourceServer"];
            _sourceUsername     = options["sourceUsername"];
            _sourcePassword     = options["sourcePassword"];
            _sourceAuthDatabase = options.Get ("sourceAuthDatabase", options["authDatabaseNameSource"]);

            _targetUri = options["target"];
            _targetServer = options["targetServer"];
            _targetUsername = options["targetUsername"];
            _targetPassword = options["targetPassword"];
            _targetAuthDatabase = options.Get ("targetAuthDatabase", options["authDatabaseNameTarget"]);

            _insertBatchSize = options.Get ("insertBatchSize", -1);
            _threads = options.Get ("threads", 1);

            _copyIndexes = options.Get ("copy-indexes", true);
            _dropCollections = options.Get ("drop-collections", false);

            // check parameter databases
            _sourceDatabases = ParseArgumentAsList (options, "sourceDatabase");
            _targetDatabases = ParseArgumentAsList (options, "targetDatabase");

            //*******************
            //** sanity checks **
            //*******************

            if (String.IsNullOrEmpty (_sourceUri) && String.IsNullOrEmpty (_sourceServer))
            {
                logger.Error ("No source mongodb server connection information provided: use the argument 'source' to provide a mongodb uri with the connection information");
                ConsoleUtils.CloseApplication (-101, true);
            }

            // TODO: target server should defaults to the source server if none is provided...
            if (String.IsNullOrEmpty (_targetUri) && String.IsNullOrEmpty (_targetServer))
            {
                logger.Error ("No source mongodb server connection information provided: use the argument 'source' to provide a mongodb uri with the connection information");
                ConsoleUtils.CloseApplication (-102, true);
            }

            if (_sourceDatabases.Count == 0)
            {
                logger.Error ("No database selected: use the argument 'sourceDatabase' to provide a list of databases");
                ConsoleUtils.CloseApplication (-103, true);
            }

            // if no target database is provided, lets use the sourcedatabases
            if (_targetDatabases.Count == 0)
            {
                _targetDatabases = null;
                // TODO: if same server... throw an error!
                // ...
            }
            else 
            {
                // if we have target database names:
                // 1. sourceDatabase cannot contain wildcard
                if (_sourceDatabases.Any (i => SharedMethods.HasWildcard (i)))
                {
                    logger.Error ("Wildcard cannot be used in source database names if a list of target databases is provided!");
                    ConsoleUtils.CloseApplication (-104, true);
                }

                // 2. sourceDatabase cannot contain wildcard
                if (_sourceDatabases.Any (i => SharedMethods.HasWildcard (i)))
                {
                    logger.Error ("Wildcard cannot be used in target database names!");
                    ConsoleUtils.CloseApplication (-105, true);
                }

                // 2. check for database mapping discrepancy
                // TODO: allow copying from multiple databases to a single database...
                if (_sourceDatabases.Count != _targetDatabases.Count)
                {
                    logger.Error ("Different number of source and target databases detected: use the argument 'sourceDatabase' and 'targetDatabases' to provide a list of databases");
                    ConsoleUtils.CloseApplication (-106, true);
                }
                
            }            

            // check collections parameter
            _collections = ParseArgumentAsList (options, "collections");
        }

        private static void CheckConnections (FlexibleOptions options)
        {
            // source server
            if (!String.IsNullOrWhiteSpace (options.Get ("source")))
            {
                if (options.Get ("source").IndexOf ("://") < 1)
                    options.Set ("source", "mongodb://" + options.Get ("source"));
                var mongoUri = new MongoUrlBuilder (options.Get ("source"));

                if (mongoUri.ConnectTimeout.TotalSeconds < 30) mongoUri.ConnectTimeout = TimeSpan.FromSeconds (30);
                if (mongoUri.SocketTimeout.TotalMinutes < 4) mongoUri.SocketTimeout = TimeSpan.FromMinutes (4);
                if (mongoUri.MaxConnectionIdleTime.TotalSeconds < 30) mongoUri.MaxConnectionIdleTime = TimeSpan.FromSeconds (30);

                // check for missing uri parameters
                if (!String.IsNullOrWhiteSpace (_sourceUsername) && String.IsNullOrWhiteSpace (mongoUri.Username))
                    mongoUri.Username = _sourceUsername;

                if (!String.IsNullOrWhiteSpace (_sourcePassword) && String.IsNullOrWhiteSpace (mongoUri.Password))
                    mongoUri.Password = _sourcePassword;

                if (!String.IsNullOrWhiteSpace (_sourceAuthDatabase) && String.IsNullOrWhiteSpace (mongoUri.AuthenticationSource))
                    mongoUri.AuthenticationSource = _sourceAuthDatabase;

                options.Set ("source", mongoUri.ToString ());
            }
            else
            {
                options.Set ("source", MongoDbContext.BuildConnectionString (_sourceUsername, _sourcePassword, true, true, _sourceServer, 30000, 4 * 60000, _sourceAuthDatabase));
            }

            // check connection
            try
            {
                MongoDbContext.GetServer (options.Get ("source")).Ping ();
            }
            catch (Exception ex)
            {
                logger.Error ("Failed to connect to source mongodb server. Uri: {0}. Details: {1}", options.Get ("source"), ex.Message);
                ConsoleUtils.CloseApplication (-111, true);
            }


            // target server
            if (!String.IsNullOrWhiteSpace (options.Get ("target")))
            {
                if (options.Get ("target").IndexOf ("://") < 1)
                    options.Set ("target", "mongodb://" + options.Get ("target"));
                var mongoUri = new MongoUrlBuilder (options.Get ("target"));

                if (mongoUri.ConnectTimeout.TotalSeconds < 30) mongoUri.ConnectTimeout = TimeSpan.FromSeconds (30);
                if (mongoUri.SocketTimeout.TotalMinutes < 4) mongoUri.SocketTimeout = TimeSpan.FromMinutes (4);
                if (mongoUri.MaxConnectionIdleTime.TotalSeconds < 30) mongoUri.MaxConnectionIdleTime = TimeSpan.FromSeconds (30);

                // check for missing uri parameters
                if (!String.IsNullOrWhiteSpace (_sourceUsername) && String.IsNullOrWhiteSpace (mongoUri.Username))
                    mongoUri.Username = _sourceUsername;

                if (!String.IsNullOrWhiteSpace (_sourcePassword) && String.IsNullOrWhiteSpace (mongoUri.Password))
                    mongoUri.Password = _sourcePassword;

                if (!String.IsNullOrWhiteSpace (_sourceAuthDatabase) && String.IsNullOrWhiteSpace (mongoUri.AuthenticationSource))
                    mongoUri.AuthenticationSource = _sourceAuthDatabase;

                options.Set ("target", mongoUri.ToString ());
            }
            else
            {
                options.Set ("target", MongoDbContext.BuildConnectionString (_targetUsername, _targetPassword, true, true, _targetServer, 30000, 4 * 60000, _targetAuthDatabase));
            }

            // check connection
            try
            {
                MongoDbContext.GetServer (options.Get ("target")).Ping ();
            }
            catch (Exception ex)
            {
                logger.Error ("Failed to connect to target mongodb server. Uri: {0}. Details: {1}", options.Get ("target"), ex.Message);
                ConsoleUtils.CloseApplication (-112, true);
            }

        }

        private static List<string> ParseArgumentAsList (FlexibleOptions options, string key)
        {
            // first check if we have a json array
            var list = options.Get<string[]> (key, null);
            // fallback to a csv string
            if (list == null)
                list = (options.Get (key, "") ?? "").Split (',', ';');
            return list.Select (i => i.Trim ()).Where (i => !String.IsNullOrEmpty (i)).ToList ();
        }
        
    }
}


