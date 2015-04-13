using CSVReflection;
using CSVReflection.Configuration;
using MongoDB;
using MongoDB.Bson;
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
        private enum ExportFormat { JSON = 0, CSV }

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
        private static ExportFormat _exportFormat;

        static void Main (string[] args)
        {
            // Args Sanity Check
            if (args == null || args.Length != 3)
            {
                Console.WriteLine ("Incorrect number of arguments received. Expected 3");
                System.Environment.Exit (-101);
            }
            
            // Loading Configuration
            LoadConfiguration ();

            // Sanity Check of Config and Arguments
            if (!ValidateConfig () || !ValidateArgs (args))
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine ("Missing MongoDB Configuration Parameter. (Server, Database, Collection and Credentials are Mandatory)");
                System.Environment.Exit (-102);
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
                System.Environment.Exit (-103);
            }

            // Loading Export Configuration from XML File
            if (!JsonToCSV.LoadExportLayout (_layoutPath))
            {
                // Error Checking
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine ("Error Loading Export Layout");
                Console.WriteLine ("Message : " + JsonToCSV.errorMessage);                
                System.Environment.Exit (-104);
            }

            // Setting up MongoDB Cursor
            MongoCursor cursor = sourceDatabase.GetCollection<BsonDocument> (_mongoCollection).Find (query);
            cursor.SetFlags (QueryFlags.NoCursorTimeout);
            
            // Counters
            int recordsProcessed = 0;
            
            // File Writer
            using (StreamWriter fWriter = new StreamWriter (_outputFile, false, Encoding.UTF8))
            {
                // Auto Flush
                fWriter.AutoFlush = true;

                // Output File Line
                string fileLine = String.Empty;

                // Iterating over documents found using the query
                foreach (BsonDocument document in cursor)
                {
                    // Picking which export method will be used
                    if (_exportFormat == ExportFormat.CSV)
                    {
                        // Extracting data from it
                        fileLine = JsonToCSV.Convert (document);
                    }
                    else
                    {
                        fileLine = document.ToString ();
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

        private static bool ValidateArgs (string[] args)
        {
            // If there are "3" arguments, the export type MUST BE "CSV".
            // If there are "2" arguments, the export type MUST BE "JSON"
            if (args.Length != 2 && args.Length != 3)
            {
                return false; // Arguments Error
            }

            // Saving Collection Value
            _mongoCollection = args[0];

            // Saving Path of XML Config/Layout for the Export
            _layoutPath      = args[1];

            // Checking whether the third value is either CSV or JSON
            string format    = args[2].ToUpper();

            if (format.Equals ("CSV") && args.Length == 3)
            {
                _exportFormat = ExportFormat.CSV;
            }
            else if (format.Equals ("JSON") && args.Length == 2)
            {
                _exportFormat = ExportFormat.JSON;
            }
            else // Wrong value of format received
            {
                return false; 
            }

            // OK
            return true;
        }
    }
}
