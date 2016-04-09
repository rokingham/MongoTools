#region *   License     *
/*
    SimpleHelpers - ConsoleUtils   

    Copyright © 2015 Khalid Salomão

    Permission is hereby granted, free of charge, to any person
    obtaining a copy of this software and associated documentation
    files (the “Software”), to deal in the Software without
    restriction, including without limitation the rights to use,
    copy, modify, merge, publish, distribute, sublicense, and/or sell
    copies of the Software, and to permit persons to whom the
    Software is furnished to do so, subject to the following
    conditions:

    The above copyright notice and this permission notice shall be
    included in all copies or substantial portions of the Software.

    THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND,
    EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
    OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
    NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
    HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
    WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
    FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
    OTHER DEALINGS IN THE SOFTWARE. 

    License: http://www.opensource.org/licenses/mit-license.php
    Website: https://github.com/khalidsalomao/SimpleHelpers.Net
 */
#endregion
 
using NLog;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace MongoToolsLib.SimpleHelpers
{
    public class ConsoleUtils
    {
        public static readonly CultureInfo cultureUS = new CultureInfo ("en-US");
        public static readonly CultureInfo cultureBR = new CultureInfo ("pt-BR");

        public static FlexibleOptions ProgramOptions { get; private set; }

        private static InitializationOptions InitOptions = null;

        public class InitializationOptions
        {
            public bool overrideNLogFileConfiguration = true;
            public List<NLog.Targets.Target> targets;

            public InitializationOptions OverrideNLogFileConfiguration (bool Override)
            {
                overrideNLogFileConfiguration = Override;
                return this;
            }

            public InitializationOptions AddNLogTarget (params NLog.Targets.Target[] Targets)
            {
                if (Targets != null)
                {
                    if (targets == null)
                        targets = new List<NLog.Targets.Target> ();
                    targets.AddRange (Targets);
                }
                return this;
            }
        }

        public static FlexibleOptions Initialize (string[] args, bool thrownOnError, InitializationOptions options = null)
        {
            InitOptions = options;
            DefaultProgramInitialization ();


            ProgramOptions = CheckCommandLineParams (args, thrownOnError);

            if (ProgramOptions.Get<bool> ("help", false) || ProgramOptions.Get<bool> ("h", false))
            {
                show_help ("");
                CloseApplication (0, true);
            }

            // display program initialization header
            if (!Console.IsOutputRedirected)
            {
                ConsoleUtils.DisplayHeader (
                    typeof(ConsoleUtils).Namespace.Replace (".SimpleHelpers", ""),
                    "options: " + (ProgramOptions == null ? "none" : "\n#    " + String.Join ("\n#    ", ProgramOptions.Options.Select (i => i.Key + "=" + i.Value))));
            }
            else
            {
                var logger = GetLogger ();
                if (logger.IsDebugEnabled)
                {
                    logger.Debug ("options: " + (ProgramOptions == null ? "none" : "\n#    " + String.Join ("\n#    ", ProgramOptions.Options.Select (i => i.Key + "=" + i.Value))));
                }
            }

            return ProgramOptions;
        }

        internal static void DefaultProgramInitialization ()
        {
            // set culture info
            // net40 or lower
            //System.Threading.Thread.CurrentThread.CurrentCulture = cultureUS;
            //System.Threading.Thread.CurrentThread.CurrentUICulture = cultureUS;
            // net45 or higher
            CultureInfo.DefaultThreadCurrentCulture = cultureUS;
            CultureInfo.DefaultThreadCurrentUICulture = cultureUS;

            // some additional configuration
            // http://stackoverflow.com/questions/8971210/how-to-turn-off-the-automatic-proxy-detection-in-the-amazons3-object
            System.Net.WebRequest.DefaultWebProxy = null;

            // more concurrent connections to the same IP (avoid throttling) and other tuning
            // http://blogs.msdn.com/b/jpsanders/archive/2009/05/20/understanding-maxservicepointidletime-and-defaultconnectionlimit.aspx
            System.Net.ServicePointManager.DefaultConnectionLimit = 1024; // more concurrent connections to the same IP (avoid throttling)
        }

        static string _logFileName;
        static string _logLevel;

        private static Logger GetLogger ()
        {
            if (_logFileName == null)
                InitializeLog (null, null, InitOptions);
            return LogManager.GetCurrentClassLogger ();
        }
        /// <summary>
        /// Log initialization.
        /// </summary>
        internal static void InitializeLog (string logFileName = null, string logLevel = null, InitializationOptions options = null)
        {
            if (options != null && !options.overrideNLogFileConfiguration && LogManager.Configuration == null)
                return;

            // default parameters initialization from config file
            if (String.IsNullOrEmpty (logFileName))
                logFileName = _logFileName ?? System.Configuration.ConfigurationManager.AppSettings["logFilename"];
			if (String.IsNullOrEmpty (logFileName))
                logFileName = ("${basedir}/log/" + typeof (ConsoleUtils).Namespace.Replace (".SimpleHelpers", "") + ".log");
            if (String.IsNullOrEmpty (logLevel))
                logLevel = _logLevel ?? (System.Configuration.ConfigurationManager.AppSettings["logLevel"] ?? "Info");

            // check if log was initialized with same options
            if (_logFileName == logFileName && _logLevel == logLevel)
                return;

            // try to parse loglevel
            LogLevel currentLogLevel;
            try { currentLogLevel = LogLevel.FromString (logLevel); }
            catch { currentLogLevel = LogLevel.Info; }

            // save current log configuration
            _logFileName = logFileName;
            _logLevel = currentLogLevel.ToString ();

            // prepare log configuration
            var config = new NLog.Config.LoggingConfiguration ();

            // console output
            if (!Console.IsOutputRedirected)
            {
                var consoleTarget = new NLog.Targets.ColoredConsoleTarget ();
                consoleTarget.Layout = "${longdate}\t${callsite}\t${level}\t${message}\t${onexception: \\:[Exception] ${exception:format=tostring}}";

                config.AddTarget ("console", consoleTarget);

                var rule1 = new NLog.Config.LoggingRule ("*", LogLevel.Trace, consoleTarget);
                config.LoggingRules.Add (rule1);
            }

            // file output
            var fileTarget = new NLog.Targets.FileTarget ();
            fileTarget.FileName = logFileName;
            fileTarget.Layout = "${longdate}\t${callsite}\t${level}\t\"${message}${onexception: \t [Exception] ${exception:format=tostring}}\"";
            fileTarget.ConcurrentWrites = true;
			fileTarget.ConcurrentWriteAttemptDelay = 10;
            fileTarget.ConcurrentWriteAttempts = 8;
            fileTarget.AutoFlush = true;
            fileTarget.KeepFileOpen = true;
            fileTarget.DeleteOldFileOnStartup = false;
            fileTarget.ArchiveAboveSize = 4 * 1024 * 1024;  // 4 Mb
            fileTarget.MaxArchiveFiles = 10;
            fileTarget.ArchiveNumbering = NLog.Targets.ArchiveNumberingMode.DateAndSequence;
            fileTarget.ArchiveDateFormat = "yyyyMMdd_HHmmss";

            // set file output to be async (commented out since doesn't work on mono)
            // var wrapper = new NLog.Targets.Wrappers.AsyncTargetWrapper (fileTarget);

            config.AddTarget ("file", fileTarget);

            // configure log from configuration file
            var rule2 = new NLog.Config.LoggingRule ("*", currentLogLevel, fileTarget);
            config.LoggingRules.Add (rule2);

            // External Log Target
            if (options != null && options.targets != null)
            {
                foreach (var t in options.targets)
                {
                    config.AddTarget (t);
                    config.LoggingRules.Add (new NLog.Config.LoggingRule ("*", currentLogLevel, t));
                }
            }

            // set configuration options
            LogManager.Configuration = config;
        }

        /// <summary>
        /// Execute some housekeeping and closes the application.
        /// </summary>
        /// <param name="exitCode">The exit code.</param>
        public static void CloseApplication (int exitCode, bool exitApplication)
        {
            System.Threading.Thread.Sleep (0);
            // log error code and close log
            if (exitCode == 0)
                GetLogger ().Debug ("ExitCode " + exitCode.ToString ());
            else
                GetLogger ().Error ("ExitCode " + exitCode.ToString ());
            LogManager.Flush ();
            System.Threading.Thread.Sleep (0);
			// force garbage collector run
            // usefull for clearing COM interfaces or any other similar resource
            GC.Collect ();
            GC.WaitForPendingFinalizers ();
            System.Threading.Thread.Sleep (0);

            // set exit code and exit
            System.Environment.ExitCode = exitCode;
            if (exitApplication)
                System.Environment.Exit (exitCode);
        }

        /// <summary>
        /// Checks the command line params.<para/>
        /// arguments format: key=value or --key value
        /// </summary>
        /// <param name="args">The args.</param>
        internal static FlexibleOptions CheckCommandLineParams (string[] args, bool thrownOnError)
        {
            FlexibleOptions mergedOptions = null;
            FlexibleOptions argsOptions = null;
            FlexibleOptions localOptions = new FlexibleOptions ();
            FlexibleOptions externalLoadedOptions = null;

            try
            {

                // parse local configuration file
                // display the options listed in the configuration file                 
                try
                {
                    var appSettings = System.Configuration.ConfigurationManager.AppSettings;
                    foreach (var k in appSettings.AllKeys)
                    {
                        localOptions.Set (k, appSettings[k]);
                    }
                }

                catch (Exception appSettingsEx)
                {
                    if (thrownOnError)
                        throw;
                    GetLogger ().Warn (appSettingsEx);
                }

                // parse console arguments
                // parse arguments like: key=value
                argsOptions = ParseCommandLineArguments (args);

                // merge arguments with app.config options. Priority: arguments > app.config
                mergedOptions = FlexibleOptions.Merge (localOptions, argsOptions);
                // adjust alias for web hosted configuration file
                if (String.IsNullOrEmpty (mergedOptions.Get ("config")))
                    mergedOptions.Set ("config", mergedOptions.Get ("S3ConfigurationPath", mergedOptions.Get ("webConfigurationFile")));

                // load and parse web hosted configuration file (priority order: argsOptions > localOptions)
                string externalConfigFile = mergedOptions.Get ("config", "");
                bool configAbortOnError = mergedOptions.Get ("configAbortOnError", true);
                if (!String.IsNullOrWhiteSpace (externalConfigFile))
                {
                    foreach (var file in externalConfigFile.Trim(' ', '\'', '"', '[', ']').Split (',', ';'))
                    {
                        GetLogger ().Debug ("Loading configuration file from {0} ...", externalConfigFile);
                        externalLoadedOptions = FlexibleOptions.Merge (externalLoadedOptions, LoadExtenalConfigurationFile (file.Trim (' ', '\'', '"'), configAbortOnError));
                    }
                }
            }
            catch (Exception ex)
            {
                // initialize log before dealing with exceptions
                if (mergedOptions != null)
                    InitializeLog (mergedOptions.Get ("logFilename"), mergedOptions.Get ("logLevel", "Info"), InitOptions);
                if (thrownOnError)
                    throw;
                GetLogger ().Error (ex);
            }

            // merge options with the following priority:
            // 1. console arguments
            // 2. web configuration file
            // 3. local configuration file (app.config or web.config)
            mergedOptions = FlexibleOptions.Merge (mergedOptions, externalLoadedOptions, argsOptions);

            // reinitialize log options if different from local configuration file
            InitializeLog (mergedOptions.Get ("logFilename"), mergedOptions.Get ("logLevel", "Info"), InitOptions);

            // return final merged options
            ProgramOptions = mergedOptions;
            return mergedOptions;
        }

        private static FlexibleOptions ParseCommandLineArguments (string[] args)
        {
            var argsOptions = new FlexibleOptions ();
            if (args != null)
            {
                string arg;
                bool openTag = false;
                string lastTag = null;
                for (int ix = 0; ix < args.Length; ix++)
                {
                    arg = args[ix];
                    // check for option with key=value sintax (restriction: the previous tag must not be an open tag)
                    // also valid for --key:value
                    bool hasStartingMarker = arg.StartsWith ("-", StringComparison.Ordinal) || arg.StartsWith ("/", StringComparison.Ordinal);
                    int p = arg.IndexOf ('=');
                    if (p > 0 && (hasStartingMarker || !openTag))
                    {
                        argsOptions.Set (arg.Substring (0, p).Trim ().TrimStart ('-', '/'), arg.Substring (p + 1).Trim ());
                        lastTag = null;
                        openTag = false;
                    }
                    // search for tag stating with special character
                    else if (hasStartingMarker)
                    {
                        lastTag = arg.Trim ().TrimStart ('-', '/');
                        argsOptions.Set (lastTag, "true");
                        openTag = true;
                    }
                    // set value of last tag
                    else if (lastTag != null)
                    {
                        argsOptions.Set (lastTag, arg.Trim ());
                        openTag = false;
                    }
                }
            }
            return argsOptions;
        }

        private static FlexibleOptions LoadExtenalConfigurationFile (string filePath, bool thrownOnError)
        {
            if (filePath.StartsWith ("http", StringComparison.OrdinalIgnoreCase))
            {
                return LoadWebConfigurationFile (filePath, thrownOnError);
            }
            else
            {
                return LoadFileSystemConfigurationFile (filePath, thrownOnError);
            }
        }

        private static FlexibleOptions LoadWebConfigurationFile (string filePath, bool thrownOnError)
        {
            using (WebClient client = new WebClient ())
            {
                try
                {
                    return parseFile (client.DownloadString (filePath));
                }
                catch (Exception ex)
                {
                    if (thrownOnError)
                        throw;
                    GetLogger ().Error (ex);
                    return new FlexibleOptions ();
                }
            }
        }

        private static FlexibleOptions LoadFileSystemConfigurationFile (string filePath, bool thrownOnError)
        {
            try
            {
            	string text = ReadFileAllText (filePath);
            	return parseFile (text);
            }
            catch (Exception ex)
            {
                if (thrownOnError)
                    throw;
                GetLogger ().Error (ex);
                return new FlexibleOptions ();
            }
        }

        private static FlexibleOptions parseFile (string content)
        {
            var options = new FlexibleOptions ();

            // detect xml
            if (content.TrimStart ().StartsWith ("<"))
            {
                var xmlDoc = System.Xml.Linq.XDocument.Parse (content);
                var root = xmlDoc.Descendants ("config").FirstOrDefault ();
                if (root != null && root.HasElements)
                {
                    foreach (var i in root.Elements ())
                    {
                        options.Set (i.Name.ToString (), i.Value);
                    }
                }
            }
            else
            {
                var json = Newtonsoft.Json.Linq.JObject.Parse (content);
                foreach (var i in json)
                {
                    options.Set (i.Key, i.Value.ToString (Newtonsoft.Json.Formatting.None));
                }
            }
            return options;
        }

        private static void show_help (string message, bool isError = false)
        {
            // files with help text in order of priority
            var files = new string[] { "help", "help.txt", "Configuration.md", "README.md" };
            var text = "Help command line arguments";
            string file = files.FirstOrDefault ();
            // check which file exists
            foreach (var f in files)
            {
                if (System.IO.File.Exists (f))
                {
                    file = f; break;
                }
                if (System.IO.File.Exists (".docs/" + f))
                {
                    file = ".docs/" + f; break;
                }
            }
            // try to load help text
            if (System.IO.File.Exists (file))
                text = ReadFileAllText (file);

            // display message parameter
            if (isError)
            {
                Console.Error.WriteLine (message);
            }
            else
            {
                Console.WriteLine (message);
            }
            
            // display help text
            Console.Error.WriteLine (text);
        }

        public static string ReadFileAllText (string filename)
        {
            // enable file encoding detection
            var encoding = SimpleHelpers.FileEncoding.DetectFileEncoding (filename);
            // Load data based on parameters
            return System.IO.File.ReadAllText (filename, encoding);
        }

        public static string[] ReadFileAllLines (string filename)
        {
            // enable file encoding detection
            var encoding = SimpleHelpers.FileEncoding.DetectFileEncoding (filename);
            // Load data based on parameters
            return System.IO.File.ReadAllLines (filename, encoding);
        }

        public static void DisplayHeader (params string[] messages)
        {
            DisplaySeparator ();

            Console.WriteLine ("#  {0}", DateTime.Now.ToString ("yyyy/MM/dd HH:mm:ss"));

            if (messages == null)
            {
                Console.WriteLine ("#  ");
            }
            else
            {
                foreach (var msg in messages)
                {
                    Console.Write ("#  ");
                    Console.WriteLine (msg ?? "");
                }
            }

            DisplaySeparator ();
            Console.WriteLine ();
        }

        public static void DisplaySeparator ()
        {
            Console.WriteLine ("##########################################");
        }

        public static void WaitForAnyKey ()
        {
            WaitForAnyKey ("Press any key to continue...");
        }

        public static void WaitForAnyKey (string message)
        {
            Console.WriteLine (message);
            Console.ReadKey ();
        }

        public static string GetUserInput (string message)
        {
            message = (message ?? String.Empty).Trim ();
            Console.WriteLine (message);
            Console.Write ("> ");
            return Console.ReadLine ();
        }

        public static IEnumerable<string> GetUserInputAsList (string message)
        {
            message = (message ?? String.Empty).Trim ();
            Console.WriteLine (message + " (enter an empty line to stop)");
            Console.Write ("> ");
            var txt = Console.ReadLine ();
            while (!String.IsNullOrEmpty (txt))
            {
                yield return txt;
                Console.Write ("> ");
                txt = Console.ReadLine ();
            }
        }

        public static char GetUserInputKey (string message = null)
        {
            message = (message ?? "Press any key to continue...").Trim ();
            Console.WriteLine (message);
            Console.Write ("> ");
            return Console.ReadKey (false).KeyChar;
        }

        public static bool GetUserInputAsBool (string message)
        {
            bool done = false;
            while (!done)
            {
                // show message
                var res = GetUserInputKey (message + " (Y/N)");
                // treat input
                if (res == 'y' || res == 'Y')
                    return true;
                if (res == 'N' || res == 'n')
                    return false;
            }
            return false;
        }

        public static int GetUserInputAsInt (string message)
        {
            int value = 0;
            bool done = false;
            while (!done)
            {
                // show message
                var res = GetUserInput (message + " (integer)").Trim ();
                // treat input
                if (int.TryParse (res, out value))
                    break;
            }
            return value;
        }

        public static double GetUserInputAsDouble (string message)
        {
            double value = 0;
            bool done = false;
            while (!done)
            {
                // show message
                var res = GetUserInput (message + " (float)").Trim ();
                // treat input
                if (double.TryParse (res, out value))
                    break;
            }
            return value;
        }
    }
}
