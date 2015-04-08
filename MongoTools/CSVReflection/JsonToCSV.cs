using CSVReflection.Configuration;
using MongoDB.Bson;
using MongoDB.Bson.IO;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Serialization;

namespace CSVReflection
{
    public class JsonToCSV
    {
        private static ExportConfig _configuration;

        #region ** Public Attributes **

        public static string errorMessage;
        public static bool   error;

        #endregion

        public static String Convert (BsonDocument jsonObject)
        {
            // Local Dictionary
            List<String> fieldValues = new List<String> ();
            
            // Json Writer Settings - To avoid problems with 10Gen types
            var jsonSettings = new JsonWriterSettings () { OutputMode = JsonOutputMode.Strict };

            // Mapping string to a dynamic json object
            JObject mappedJson = JObject.Parse (jsonObject.ToJson (jsonSettings));
            
            // Trying to extract property values out of the object
            foreach (Field field in _configuration.Fields)
            {
                // Field Data Placeholder
                JToken fieldData;

                // Field Value placeholder
                string fieldValue;
                
                // Checking for the situation where the field is nested
                if (field.Name.Contains('.'))
                {
                    // Checking if at least, the root node, exists
                    String[] fields = field.Name.Split ('.');

                    fieldData = mappedJson[fields[0]];

                    // Splits the field name into an array, so that we can drew down the fields for the last one
                    foreach (var fieldHierarchy in fields.Skip (1))
                    {
                        fieldData = fieldData[fieldHierarchy];
                    }
                }
                else
                {
                    // JObject with field data
                    fieldData = mappedJson[field.Name];
                }                

                // Checking for "Field not found"
                if (fieldData == null)
                {
                    // Is this field mandatory ?
                    if (field.Mandatory)
                    {
                        // Aborts, because this field should be on the field
                        return null;
                        
                    }
                    else // If not, just set the string to empty and adds it to the output
                    {
                        fieldValue = String.Empty;
                    }
                }
                else
                {
                    // Checking for JToken Type
                    JTokenType objType = fieldData.Type;
                    
                    // Sanity Check for NULL Values of properties that do exist
                    if (objType == JTokenType.Null)
                    {
                        fieldValue = String.Empty;
                    }
                    else if (objType == JTokenType.Array) // Checking for Arrays (that need to be serialized differently)
                    {
                        String[] valuesArray = fieldData.Select (t => t.Value<String> ().Replace (_configuration.ListDelimiter, String.Empty)
                                                                                        .Replace (_configuration.Delimiter    , String.Empty)).ToArray ();

                        fieldValue           = String.Join (_configuration.ListDelimiter, valuesArray);
                    }
                    else if (objType == JTokenType.Object && field.Name.Equals ("_id")) // Checking for specific MongoDB "_id" situation
                    {
                        fieldValue = fieldData.ToObject<String> (); // Value<ObjectId> ().ToString ();
                    }
                    else
                    {
                        // Reaching Attribute Value
                        fieldValue = fieldData.Value<String> ();
                    }
                }

                // Adding Key and Value to the dictionary
                fieldValues.Add (fieldValue.Replace (_configuration.Delimiter, String.Empty));
            }

            // Returning Joined list as a big string
            return String.Join (_configuration.Delimiter, fieldValues);
        }
        
        public static bool LoadExportLayout (string xmlPath)
        {
            // XML Deserializer
            XmlSerializer deserializer = new XmlSerializer (typeof (ExportConfig));
            StreamReader xmlReader     = new StreamReader (xmlPath);

            // Reading Content
            try
            {
                _configuration = (ExportConfig) deserializer.Deserialize (xmlReader);
                return true;
            }
            catch (Exception ex)
            {
                // Control Variables
                error = true;
                errorMessage = ex.Message;

                // Null Value - Error
                _configuration =  null;
                return false;
            }
        }
    }
}
