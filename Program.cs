namespace PeeringDB.Console
{
    using CachingGeolocation;
    using CsvHelper; //// https://joshclose.github.io/CsvHelper/
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.SQLite; //// https://system.data.sqlite.org/index.html/doc/trunk/www/index.wiki
    using System.Globalization;
    using System.IO;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;

    class Program
    {
        static void Main(string[] args)
        {
            var rootPath = @"c:\Projects\PeeringDB\";
            string datePrefix = "2019-01-05";
            var sqliteFilename = string.Format("{0}-peeringdb.sqlite3", datePrefix); // 2017-08-20-peeringdb.sqlite3
            var sqlitePath = Path.Combine(rootPath, sqliteFilename); // c:\Projects\PeeringDB\2017-08-20-peeringdb.sqlite3
            var connectionString = string.Format(CultureInfo.InvariantCulture, @"Data Source={0}", sqlitePath); // Data Source=c:\Projects\PeeringDB\2017-08-20-peeringdb.sqlite3

            var tableNames = ListTables(connectionString);
            
            foreach (var tableName in tableNames)
            {
                ExportTableToTSV(
                    connectionString: connectionString,
                    tsvOutputPath: Path.Combine(rootPath, string.Format(CultureInfo.InvariantCulture, "{0}-{1}.tsv", datePrefix, tableName)),
                    tableName: tableName);
            }
        }

        private static void ExportTableToTSV(string connectionString, string tsvOutputPath, string tableName)
        {
            using (var sqlite = new SQLiteConnection(connectionString))
            {
                using (var cmd = sqlite.CreateCommand())
                {
                    cmd.CommandText = string.Format(CultureInfo.InvariantCulture, "SELECT * FROM {0};", tableName);

                    var dataTable = new DataTable();
                    var adapter = new SQLiteDataAdapter(cmd);
                    adapter.Fill(dataTable);

                    using (var writer = new StreamWriter(tsvOutputPath))
                    {
                        using (var csvWriter = new CsvWriter(writer))
                        {
                            csvWriter.Configuration.Delimiter = "\t";

                            // We cannot use var instead of DataRow here
                            foreach (DataRow row in dataTable.Rows)
                            {
                                for (var i = 0; i < dataTable.Columns.Count; i++)
                                {
                                    var item = row[i];

                                    if (item is string && item != null)
                                    {
                                        var strItem = item as string;
                                        strItem = strItem.Replace("\r\n", " ").Replace("\r", " ").Replace("\n", " ");
                                        csvWriter.WriteField(strItem);
                                    }
                                    else
                                    {
                                        csvWriter.WriteField(row[i]);
                                    }
                                }

                                csvWriter.NextRecord();
                            }
                        }
                    }
                }
            }
        }

        private static List<string> ListTables(string connectionString)
        {
            var tableNames = new List<string>();

            using (var sqlite = new SQLiteConnection(connectionString))
            {
                using (var cmd = sqlite.CreateCommand())
                {
                    cmd.CommandText = "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY 1";

                    var dataTable = new DataTable();
                    var adapter = new SQLiteDataAdapter(cmd);
                    adapter.Fill(dataTable);

                    foreach (DataRow row in dataTable.Rows)
                    {
                        var tableName = row["name"] as string;

                        // Ignore sqlite_* tables
                        if (!string.IsNullOrWhiteSpace(tableName) && !tableName.StartsWith("sqlite_"))
                        {
                            tableNames.Add(tableName);
                        }
                    }
                }
            }

            return tableNames;
        }
    }
}
