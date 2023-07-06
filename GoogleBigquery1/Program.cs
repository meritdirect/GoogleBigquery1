using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Google.Api.Gax;
using Google.Apis.Auth.OAuth2;
using Google.Apis.Bigquery.v2.Data;
using Google.Cloud.BigQuery.V2;
using System.Configuration;

namespace GoogleBigquery1
{
    class Program
    {
        static int Main(string[] args)
        {
            if (args.Length == 0)
            {
                Console.WriteLine("Invalid args");
                return 1;
            }
            string sqlString = args[0];
            string outputFile = args[1];
            int rowCount = 0;
            int debug = 0;
            int ret = 0;

            debug = int.Parse(ConfigurationManager.AppSettings["debug"]);

            LogError(sqlString);
            LogError(outputFile);

            ret = runQuery(sqlString, outputFile, debug);
            if (ret != 0)
            {
                System.Threading.Thread.Sleep(60000);
                ret = runQuery(sqlString, outputFile, debug);
            }
            return ret;

            /*
            try { 
            var credential = GoogleCredential.FromFile(ConfigurationManager.AppSettings["certFile"]);
            
            BigQueryClient client = BigQueryClient.Create(ConfigurationManager.AppSettings["projectID"], credential);
            



            
            BigQueryParameter[] parameters = null;
            BigQueryResults results = client.ExecuteQuery(sqlString, parameters);

            

            List<string> fields = new List<string>();


            string lineHeader = "";
            var delimiter = "\t";

            foreach (var col in results.Schema.Fields)
            {
                lineHeader += col.Name + delimiter;
                fields.Add(col.Name);
            }


            lineHeader = lineHeader.Substring(0, lineHeader.Length - 1);


            using (var writer = new StreamWriter(outputFile))
            {
                writer.WriteLine(lineHeader);
                foreach (BigQueryRow row in results)
                {
                        rowCount += 1;
                        if(rowCount % 10000 == 0 && debug != 0)
                        {
                            Console.WriteLine(rowCount.ToString("#,###"));
                        }
                    string line = "";
                    foreach (var col in fields)
                    {
                        
                        //line += row[col] + delimiter;

                        line += (row[col] ?? "").ToString().Replace(delimiter, " ").Replace("\n"," ").Replace("\r","") + delimiter;



                    }

                    line = line.Substring(0, line.Length - 1); //trim trailing delimiter
                    
                    writer.WriteLine(line);                   

                }
                
            }

                Console.WriteLine(rowCount.ToString());
                

            return 0;
            
        } //End Try
            catch (Exception ex)
            {
                LogError(ex.ToString());
                return 1;
            }
            */
        } //End Main
        internal static void LogError(string sText)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append(DateTime.Now.ToString("yyyy'-'MM'-'dd' 'HH':'mm':'ss") + ' ' + sText);
            File.AppendAllText(ConfigurationManager.AppSettings["logPath"] + "GoogleBigQueryLog" + DateTime.Now.ToString("yyyyMMdd") + ".txt", sb.ToString() + "\r\n");
            sb.Clear();
        }
        internal static int runQuery(string sqlString, string outputFile, int debug)
        {
            int rowCount = 0;
            try { 
            var credential = GoogleCredential.FromFile(ConfigurationManager.AppSettings["certFile"]);

            BigQueryClient client = BigQueryClient.Create(ConfigurationManager.AppSettings["projectID"], credential);





            BigQueryParameter[] parameters = null;
            BigQueryResults results = client.ExecuteQuery(sqlString, parameters);



            List<string> fields = new List<string>();


            string lineHeader = "";
            var delimiter = "\t";

            foreach (var col in results.Schema.Fields)
            {
                lineHeader += col.Name + delimiter;
                fields.Add(col.Name);
            }


            lineHeader = lineHeader.Substring(0, lineHeader.Length - 1);


            using (var writer = new StreamWriter(outputFile))
            {
                writer.WriteLine(lineHeader);
                foreach (BigQueryRow row in results)
                {
                    rowCount += 1;
                    if (rowCount % 10000 == 0 && debug != 0)
                    {
                        Console.WriteLine(rowCount.ToString("#,###"));
                    }
                    string line = "";
                    foreach (var col in fields)
                    {

                        //line += row[col] + delimiter;

                        line += (row[col] ?? "").ToString().Replace(delimiter, " ").Replace("\n", " ").Replace("\r", "") + delimiter;



                    }

                    line = line.Substring(0, line.Length - 1); //trim trailing delimiter

                    writer.WriteLine(line);

                }

            }

            Console.WriteLine(rowCount.ToString());


            return 0;

        } //End Try
            catch (Exception ex)
            {
                LogError(ex.ToString());
                return 1;
            }
}

    }// End Class
}//End Namespace
