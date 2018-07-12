using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Text;

namespace CPREnvoy
{
    public partial class CPRDataAnalysis
    {
        private static SqlConnection sqlConnection;
        private static Config config;
        private static Logger logger;

        public CPRDataAnalysis(Config eConfig, Logger logger)
        {
            CPRDataAnalysis.config = eConfig;
            CPRDataAnalysis.logger = logger;
        }

        public void Init()
        {
            CPRDataAnalysis.sqlConnection = new SqlConnection(CPRDataAnalysis.config.cprDatabaseConnectionString);

            try
            {
                CPRDataAnalysis.sqlConnection.Open();

                this.ensure_ecpr_diff_table_is_created();
                this.ensure_clones_diff_tables();
            }
            catch(Exception error)
            {
                CPRDataAnalysis.logger.write( error.Message, Logger.LOGLEVEL.ERROR);
            }
        }

        public static void compareDataDiff(string tableName, Dictionary<string, object> data, string changed_on)
        {

            if (CPRDataAnalysis.sqlConnection.State == ConnectionState.Closed)
            {
                try
                {
                    CPRDataAnalysis.sqlConnection.Open();
                }
                catch (Exception)
                {
                    return;
                }
            }

            var needStop = DateTime.Compare(CPRDataAnalysis.config.diff_stop, DateTime.UtcNow);

            if (needStop <= 0)
                return;

            try
            {

                var sqlDataReaderCount = Convert.ToInt32(CPRDataAnalysis.executeQueryScalar("SELECT COUNT(*) FROM ecpr_diff"));

                if (sqlDataReaderCount >= CPRDataAnalysis.config.diff_max)
                    return;

                int change_id = Convert.ToInt32(CPRDataAnalysis.executeQueryScalar("SELECT COALESCE(MAX(change_id), 0) + 1 as maxChangeID FROM ecpr_diff"));

                if (CPRDataAnalysis.config.diff_tables.ContainsKey(tableName))
                {
                    string pk_val = (string)CPRDataAnalysis.config.diff_tables[tableName]["pk"];
                    var data_val = data[pk_val];

                    string originTableQuery = String.Format("SELECT * FROM ecpr_{0}_clone where {1}={2}", tableName, pk_val, data_val);
                    string insertToEcprDiffTable = "INSERT INTO ecpr_diff ([table], change_id, [column], original, new, changed_on) VALUES ('{0}', {1}, '{2}', '{3}', '{4}', '{5}')";
                    List<string> insertToEcprDiffTableList = new List<string>();

                    SqlDataReader sqlDataReader = CPRDataAnalysis.executeQuery(originTableQuery);
                    if (sqlDataReader != null && sqlDataReader.HasRows)
                    {
                        while (sqlDataReader.Read())
                        {
                            foreach (var d in data)
                            {
                                var originVal = sqlDataReader[d.Key];
                                var changed_on_dt = new DateTime(1970, 1, 1, 0, 0, 0).ToLocalTime();
                                changed_on_dt = changed_on_dt.AddSeconds(Convert.ToInt64(changed_on));

                                if (!originVal.Equals(d.Value))
                                {
                                    insertToEcprDiffTableList.Add(String.Format(insertToEcprDiffTable, tableName, change_id, d.Key, originVal, d.Value,
                                        changed_on_dt.ToString("MM/dd/yyyy hh:mm:ss tt")));
                                }
                            }

                            change_id++;
                        }
                        sqlDataReader.Close();

                        foreach(var item in insertToEcprDiffTableList)
                        {
                            CPRDataAnalysis.executeNonQuery(item);
                        }
                    }
                }


            }catch(Exception error)
            {
                CPRDataAnalysis.logger.write(error.Message, Logger.LOGLEVEL.INFO);
            }

        }

        private void ensure_clones_diff_tables()
        {
            foreach (var tableInfo in CPRDataAnalysis.config.diff_tables)
            {
                var cloneQuery = String.Format("SELECT * INTO ecpr_{0}_clone FROM {0}", tableInfo.Key);
                if (CPRDataAnalysis.executeNonQuery(cloneQuery))
                {
                    CPRDataAnalysis.logger.write( String.Format("{0} is cloned to ecpr_{0}_clone.", tableInfo.Key), Logger.LOGLEVEL.INFO);
                }
            }
        }

        private void ensure_ecpr_diff_table_is_created()
        {
            var createEcprDiffTableQuery = @"
                    SET ANSI_NULLS ON
                    SET QUOTED_IDENTIFIER ON
                    SET ANSI_PADDING ON

                    CREATE TABLE [dbo].[ecpr_diff](
                      [table] [varchar](max) NOT NULL,
                      [change_id] [int] NOT NULL,
                      [column] [varchar](max) NOT NULL,
                      [original] [varchar](max) NULL,
                      [new] [varchar](max) NULL,
                      [changed_on] [datetime] NOT NULL
                    ) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]


                    SET ANSI_PADDING OFF
            ";

            if (CPRDataAnalysis.executeNonQuery(createEcprDiffTableQuery))
            {
                CPRDataAnalysis.logger.write( "ecpr_diff table is created.", Logger.LOGLEVEL.INFO);
            }
        }

        private static SqlDataReader executeQuery(string cText, CommandBehavior cBehavior = CommandBehavior.Default)
        {
            using (SqlCommand sqlCommand = CPRDataAnalysis.sqlConnection.CreateCommand())
            {
                sqlCommand.CommandText = cText;
                sqlCommand.CommandType = CommandType.Text;
                sqlCommand.CommandTimeout = 60000;
                sqlCommand.Notification = null;

                try
                {
                    return sqlCommand.ExecuteReader(cBehavior);
                }
                catch (SqlException error)
                {
                    if (error.Number == -1)
                    {
                        /* A transport-level error has occurred when sending the request to the server.
                         * (provider: Session Provider, error: 19 - Physical connection is not usable) */
                    }
                    else
                        CPRDataAnalysis.logger.write("ERROR: " + error.Message, Logger.LOGLEVEL.ERROR);
                }

            }

            return null;
        }

        private static object executeQueryScalar(string cText)
        {
            using (SqlCommand sqlCommand = CPRDataAnalysis.sqlConnection.CreateCommand())
            {
                sqlCommand.CommandText = cText;
                sqlCommand.CommandType = CommandType.Text;
                sqlCommand.CommandTimeout = 60000;
                sqlCommand.Notification = null;
                try
                {
                    return sqlCommand.ExecuteScalar();
                }
                catch (SqlException error)
                {
                    if (error.Number == -1)
                    {
                        /* A transport-level error has occurred when sending the request to the server.
                         * (provider: Session Provider, error: 19 - Physical connection is not usable) */
                    }
                    else
                        CPRDataAnalysis.logger.write(error.Message, Logger.LOGLEVEL.ERROR);
                }
            }

            return null;
        }

        private static bool executeNonQuery(string cText)
        {
            using (SqlCommand sqlCommand = CPRDataAnalysis.sqlConnection.CreateCommand())
            {
                sqlCommand.CommandText = cText;
                sqlCommand.CommandType = CommandType.Text;
                sqlCommand.Notification = null;
                try
                {
                    sqlCommand.ExecuteNonQuery();
                    return true;
                }
                catch (SqlException error)
                {
                    if (error.Number == -1)
                    {
                        /* A transport-level error has occurred when sending the request to the server.
                         * (provider: Session Provider, error: 19 - Physical connection is not usable) */
                    }
                    else
                    if (!error.Message.Contains("already an object"))
                    {
                        CPRDataAnalysis.logger.write(error.Message, Logger.LOGLEVEL.ERROR);
                        return false;
                    }
                    return true;
                }
            }
        }
    }
}
