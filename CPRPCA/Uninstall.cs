using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;

namespace CPREnvoy
{
    public class Uninstall
    {
        private Config config;

        public Uninstall(Config config)
        {
            this.config = config;
        }

        ~Uninstall()
        {
            this.config = null;
        }

        private void Drop(string dropQuery, string dropTmp, SqlConnection sqlConnection)
        {
            using (SqlDataReader sqlDataReader = this.doSQL(dropQuery, sqlConnection, true))
            {
                if (sqlDataReader.HasRows)
                {
                    while (sqlDataReader.Read())
                    {
                        this.doSQL(String.Format(dropTmp, sqlDataReader["NAME"].ToString()), sqlConnection);
                    }
                }
            }
        }

        private void removesLogTables(SqlConnection sqlConnection)
        {
            string fetchLogTables = @"
                SELECT '['+SCHEMA_NAME(schema_id)+'].['+name+']' as NAME
                FROM sys.tables
                WHERE name like 'ecpr_%';
            ";

            this.Drop(fetchLogTables, "DROP TABLE {0}", sqlConnection);
        }

        private void removesTriggers(SqlConnection sqlConnection)
        {
            string fetchTriggers = @"
                SELECT '[' + s.name  + '].[' + sysobjects.name + ']' as NAME
                FROM sysobjects
                INNER JOIN sys.tables t ON sysobjects.parent_obj = t.object_id
                INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                WHERE sysobjects.type = 'TR'  and sysobjects.name like 'ecpr_%';
            ";

            this.Drop(fetchTriggers, "DROP TRIGGER {0}", sqlConnection);
        }

        public void runTasks()
        {
            var sqlConnection = new SqlConnection(this.config.cprDatabaseConnectionString);

            try
            {
                sqlConnection.Open();

                removesTriggers(sqlConnection);
                removesLogTables(sqlConnection);
            }
            catch (Exception error)
            {
                Console.WriteLine();
                Console.WriteLine("ERROR: {0}", error.Message);
            }
            finally
            {
                if (sqlConnection.State == ConnectionState.Open)
                    sqlConnection.Close();
            }
        }

        private SqlDataReader doSQL(string qStr, SqlConnection sqlConnection, bool returns=false)
        {
            try
            {
                using (SqlCommand sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = qStr;
                    sqlCommand.CommandType = CommandType.Text;
                    if (returns)
                    {
                        return sqlCommand.ExecuteReader(CommandBehavior.Default);
                    }
                    else
                        sqlCommand.ExecuteNonQuery();
                }
            }
            catch (Exception error)
            {
                Console.WriteLine();
                Console.WriteLine("ERROR: {0}", error.Message);
            }

            return null;
        }
    }
}
