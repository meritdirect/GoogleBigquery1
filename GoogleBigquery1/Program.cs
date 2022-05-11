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

            try { 
            var credential = GoogleCredential.FromFile(ConfigurationManager.AppSettings["certFile"]);
            //var credential = GoogleCredential.FromFile("C:\\Users\\jbarash\\Downloads\\Brady\\cert\\wpsdataprd-db2cc75baf3d.json");
            BigQueryClient client = BigQueryClient.Create(ConfigurationManager.AppSettings["projectID"], credential);
            //BigQueryClient client = BigQueryClient.Create("wpsdataprd", credential);



            string sql = $"SELECT visitor_id, visitor_property_Offline_Contact_Email_Address, event_time, event_page_url_full_url, event_udo_page_category_name, event_udo_product_category, RowUpdatedDt   From itdataprd.WPSDB01.Tealium_setonus_Bigtable Where visitor_property_Offline_Contact_Email_Address Is Not Null  AND RowUpdatedDt >= CAST('2022-04-23' AS TIMESTAMP) LIMIT 1";
            //string sql = $"SELECT * From itdataprd.WPSDB01.Tealium_setonat_Bigtable Where visitor_property_Offline_Contact_Email_Address Is Not Null  AND RowUpdatedDt >= CAST('2022-05-02' AS TIMESTAMP) LIMIT 1";
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
                    string line = "";
                    foreach (var col in fields)
                    {
                        line += row[col] + delimiter;
                        
                    }

                    line = line.Substring(0, line.Length - 1); //trim trailing tab
                    
                    writer.WriteLine(line);
                   

                }
                
            }



            return 0;
            
        } //End Try
            catch (Exception ex)
            {
                return 1;
            }
        } //End Main
        internal static void LogError(string sText)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append(DateTime.Now.ToString("yyyy'-'MM'-'dd' 'HH':'mm':'ss") + ' ' + sText);
            File.AppendAllText(ConfigurationManager.AppSettings["logPath"] + "GoogleBigQueryLog" + DateTime.Now.ToString("yyyyMMdd") + ".txt", sb.ToString() + "\r\n");
            sb.Clear();
        }

    }// End Class
}//End Namespace
