using CSVReflection;
using CSVReflection.Configuration;
using MongoDB;
using MongoDB.Bson;
using MongoDB.Bson.IO;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Serialization;

namespace Export
{
    class Export
    {
        // Enum of Possible "Export Formats"
        private enum ExportFormat { JSON = 0, CSV, WRONG_FORMAT }

        // Mongo Related Attributes
        private static string _sourceServer;
        private static string _sourceDatabase;
        private static string _sourceAuthDatabase;
        private static string _sourceUsername;
        private static string _sourcePassword;
        private static string _mongoDbQuery;
        private static string _mongoCollection;

        // Export Config
        private static string       _layoutPath;
        private static string       _outputFile;
        private static int          _limit;
        private static bool         _addHeaders;
        private static ExportFormat _exportFormat;

        static void Main (string[] args)
        {
            // Args Sanity Check
            if (args == null || args.Length == 0)
            {
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
                        
            // Loading Configuration
            LoadConfiguration ();

            // Parsing Arguments
            ParseArguments (args);

            // Printing Configurations
            PrintConfiguration ();

            // Prompts for user Input
            Console.WriteLine ("Is the configuration correct ? Y/N");
            var key = Console.ReadKey ().Key;

            // Checking Key
            if (key == ConsoleKey.N) // N = "NO"
            {
                Console.WriteLine (" => 'NO' : Aborting");
                System.Environment.Exit (-102);
            }
            else if (key != ConsoleKey.Y) // Anything other than "N" and "Y" is an error.
            {
                Console.WriteLine (" => 'Wrong Key Pressed' : Expected either 'Y' or 'N'");
                System.Environment.Exit (-102);
            }

            Console.WriteLine (" => Proceeding with Export.");

            // Sanity Check of Config and Arguments
            if (!ValidateConfig ())
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine ("Missing MongoDB Configuration Parameter. (Server, Database, Collection and Credentials are Mandatory)");
                System.Environment.Exit (-103);
            }

            // Creating instance of MongoDB
            String sourceConnString      = MongoDbContext.BuildConnectionString (_sourceUsername, _sourcePassword, _sourceServer, _sourceAuthDatabase);

            // Reaching Databases
            MongoDatabase sourceDatabase = MongoDbContext.GetServer (sourceConnString).GetDatabase (_sourceDatabase);

            // Assembling "Query" to MongoDB, if any query text was provided
            QueryDocument query          = String.IsNullOrWhiteSpace (_mongoDbQuery) ? null : new QueryDocument (QueryDocument.Parse (_mongoDbQuery));

            // Checking if the provided Collection Exists
            if (!sourceDatabase.CollectionExists (_mongoCollection))
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine ("Collection [ " + _mongoCollection + " ] does not exists on the specified database");
                System.Environment.Exit (-104);
            }

            if (_exportFormat == ExportFormat.CSV)
            {
                // Loading Export Configuration from XML File
                if (!JsonToCSV.LoadExportLayout (_layoutPath))
                {
                    // Error Checking
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine ("Error Loading Export Layout");
                    Console.WriteLine ("Message : " + JsonToCSV.errorMessage);
                    System.Environment.Exit (-105);
                }
            }

            // Setting up MongoDB Cursor
            MongoCursor cursor = sourceDatabase.GetCollection<BsonDocument> (_mongoCollection).Find (query);
            cursor.SetFlags (QueryFlags.NoCursorTimeout);

            // Checking for the need to apply limit
            if (_limit != -1)
            {
                cursor.SetLimit (_limit);
            }
            
            // Counters
            int recordsProcessed = 0;

            // JSON Settings to keep the "JSON" output as "Strict"
            var jsonSettings = new JsonWriterSettings () { OutputMode = JsonOutputMode.Strict };

            // File Writer
            using (StreamWriter fWriter = new StreamWriter (_outputFile, false, Encoding.UTF8))
            {
                // Auto Flush
                fWriter.AutoFlush = true;

                // Output File Line
                string fileLine = String.Empty;

                // Should we add headers to the output CSV file?
                if (_exportFormat == ExportFormat.CSV && _addHeaders)
                {
                    // Writing Headers
                    fWriter.WriteLine (JsonToCSV.Fields);
                }

                // Iterating over documents found using the query
                foreach (BsonDocument document in cursor)
                {
                    // Picking which export method will be used
                    if (_exportFormat == ExportFormat.CSV)
                    {
                        // Extracting data from it
                        fileLine = JsonToCSV.BsonToCSV (document);
                    }
                    else
                    {
                        fileLine = document.ToJson (jsonSettings);
                    }

                    // Checking for errors
                    if (String.IsNullOrWhiteSpace (fileLine))
                    {
                        continue;
                    }

                    // Writing to output csv
                    fWriter.WriteLine (fileLine);

                    // Counting
                    if (recordsProcessed++ % 100 == 0)
                    {
                        Console.WriteLine ("Processed : " + recordsProcessed);
                    }
                }
            }
        }

        /// <summary>
        /// Reads the data out of the .Config file
        /// </summary>
        private static void LoadConfiguration ()
        {
            _sourceServer       = ConfigurationManager.AppSettings["sourceServer"];
            _sourceUsername     = ConfigurationManager.AppSettings["sourceUsername"];
            _sourcePassword     = ConfigurationManager.AppSettings["sourcePassword"];
            _sourceDatabase     = ConfigurationManager.AppSettings["sourceDatabaseName"];
            _sourceAuthDatabase = ConfigurationManager.AppSettings["authDatabaseName"];
            _mongoDbQuery       = ConfigurationManager.AppSettings["mongoQuery"];
            _outputFile         = ConfigurationManager.AppSettings["outputCSV"];
        }

        /// <summary>
        /// Parses CLI arguments
        /// </summary>
        /// <param name="args">List of Args received by the CLI</param>
        private static void ParseArguments (string[] args)
        {
            // Reaching "Export Type" parameter
            if (args.Where (t => t.Equals (Args.FORMAT_PARAMETER)).FirstOrDefault () != null)
            {
                // Reaching the index of the "Format" parameter received
                int formatIndex = GetArgumentIndex (args, Args.FORMAT_PARAMETER) + 1;

                // Checking whether the third value is either CSV or JSON
                string format    = args[formatIndex].ToUpper();

                if (format.Equals ("CSV"))
                {
                    _exportFormat = ExportFormat.CSV;
                }
                else if (format.Equals ("JSON"))
                {
                    _exportFormat = ExportFormat.JSON;
                }
                else
                {
                    // Wrong Format
                    Console.WriteLine("Wrong value of '-format' parameter received. Expected either : JSON or CSV");
                    System.Environment.Exit (-101);
                }
            }
            else // No "Format" parameter -> Default is "JSON"
            {
                _exportFormat = ExportFormat.JSON;
            }

            // Checking for "Collection-Name" Parameter
            if (args.Where (t => t.Equals (Args.COLLECTION_NAME)).FirstOrDefault() != null)
            {
                // Reaching the index of the "Collection-Name" parameter received
                int formatIndex = GetArgumentIndex (args, Args.COLLECTION_NAME) + 1;
                
                // Saving "Collection Name" value
                _mongoCollection = args[formatIndex];
            }
            else // Error - No Collection-Name received
            {
                Console.WriteLine ("No value of '-collection' received.");
                System.Environment.Exit (-101);
            }

            // "Export Layout Configuration"
            if (args.Where (t => t.Equals (Args.EXPORT_LAYOUT_PATH)).FirstOrDefault() != null)
            {
                // If any "Export Layout" was supplied, it means that the "export-format" should be "CSV". But is it ?
                if (_exportFormat != ExportFormat.CSV)
                {
                    // Error, it's not "CSV"
                    Console.WriteLine ("Received '-export-config-path' parameter, but the '-format' is 'JSON'. Only 'CSV' supports a configuration file");
                    System.Environment.Exit (-101);
                }

                // Reching the index of the "Export-Config-Path" parameter received
                int layoutConfigIndex = GetArgumentIndex (args, Args.EXPORT_LAYOUT_PATH) + 1;

                // Saving "Export Config Path" value
                _layoutPath = args[layoutConfigIndex];
            }
            else // No "Export-Config-Path" received
            {
                // If there's no "Config" Path, the export format should be "JSON". But is it ?
                if (_exportFormat != ExportFormat.JSON)
                {
                    // Error, it's not "CSV"
                    Console.WriteLine ("Haven't received '-export-config-path' parameter, but the '-format' is 'CSV'. All 'CSV' exports need a configuration file");
                    System.Environment.Exit (-101);
                }
            }

            // Checking for "Limit" Parameter
            if (args.Where (t => t.Equals (Args.EXPORT_LIMIT)).FirstOrDefault () != null)
            {
                // Reaching the index of the "Export-Limit" parameter received
                int limitIndex = GetArgumentIndex (args, Args.EXPORT_LIMIT) + 1;

                // Converting it to Int32
                _limit = Convert.ToInt32 (args[limitIndex]);
            }         
            else // No Limit, Initializes it with -1
            {
                _limit = -1;
            }

            // Checking for the "AddHeaders" Parameter
            if (args.Where (t => t.Equals (Args.ADD_HEADERS)).FirstOrDefault() != null)
            {
                _addHeaders = true;
            }
            else
            {
                _addHeaders = false;
            }
        }
        
        /// <summary>
        /// Checks the minimum parameters of this application
        /// </summary>
        /// <returns></returns>
        private static bool ValidateConfig ()
        {
            if (String.IsNullOrEmpty (_sourceServer) 
             || String.IsNullOrEmpty (_sourceUsername)
             || String.IsNullOrEmpty (_sourcePassword)
             || String.IsNullOrEmpty (_sourceDatabase))
            {
                return false;
            }

            return true;
        }

        /// <summary>
        /// Prints the application parameters
        /// </summary>
        private static void PrintHelp ()
        {
            Console.WriteLine ("***********************************");
            Console.WriteLine ("List of Parameters");
            Console.WriteLine ("\t-h : Shows Help");
            Console.WriteLine ("\t-format (defaults to 'JSON'): Either 'CSV' or 'JSON' is accepted");
            Console.WriteLine ("\t-collection : Name of the collection that will be exported (no default value here. This is mandatory)");
            Console.WriteLine ("\t-export-config-path : In case your export is 'CSV', this is needed. It's not needed for JSON exports");
            Console.WriteLine ("***********************************");
        }

        /// <summary>
        /// Prints the current configuration of the process.
        /// Enables double checking of the user before executing it
        /// </summary>
        private static void PrintConfiguration ()
        {
            Console.WriteLine ("=============================================================");
            Console.WriteLine ("Format:"      + _exportFormat);
            Console.WriteLine ("Collection:"  + _mongoCollection);
            Console.WriteLine ("Layout File:" + _layoutPath);
            Console.WriteLine ("=============================================================\n\n");
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
