using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CPREnvoy
{
    static class TestDB
    {
        public static Dictionary<string, object> runSQLQuery(Config config, string sqlQuery)
        {
            SqlTransaction trans;

            Dictionary<string, object> data = new Dictionary<string, object>();

            using (SqlConnection sqlConnection = new SqlConnection(config.cprDatabaseConnectionString))
            {
                sqlConnection.Open();

                trans = sqlConnection.BeginTransaction(IsolationLevel.ReadUncommitted);

                using (SqlCommand sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = sqlQuery;
                    sqlCommand.CommandType = CommandType.Text;
                    sqlCommand.CommandTimeout = config.defaultSQLQueryTimeout;
                    sqlCommand.Transaction = trans;

                    SqlDataReader sqlDataReader = sqlCommand.ExecuteReader();

                    sqlDataReader.Read();

                    for (int lp = 0; lp < sqlDataReader.FieldCount; lp++)
                    {
                        data.Add(sqlDataReader.GetName(lp), sqlDataReader.GetValue(lp));
                    }
                }
            }

            return data;
        }

        public static void runSQLNoneQuery(Config config, string sqlQuery, Dictionary<string, object> values = null)
        {
            SqlTransaction trans;

            using (SqlConnection sqlConnection = new SqlConnection(config.cprDatabaseConnectionString))
            {
                sqlConnection.Open();

                trans = sqlConnection.BeginTransaction();

                using (SqlCommand sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = sqlQuery;
                    sqlCommand.CommandType = CommandType.Text;
                    sqlCommand.CommandTimeout = config.defaultSQLQueryTimeout;
                    sqlCommand.Transaction = trans;

                    if (values != null)
                    {
                        foreach(var value in values)
                        {
                            sqlCommand.Parameters.AddWithValue(value.Key, value.Value);
                        }
                    }

                    sqlCommand.ExecuteNonQuery();
                    trans.Commit();
                }
            }
        }
    }

    static class TestQuery
    {
        public static readonly Dictionary<string, List<string>> TEST_QUERY
            = new Dictionary<string, List<string>> {
    {"DOCTORS", new List<string> { "PH_LAST"} },
    {"DRUGALLR", new List<string> { "MRN", "CPK_DRUGALLR" } },
    {"HOSPITAL", new List<string> { "TEXT_"} },
    {"HR", new List<string> { "MRN", "FIRST_NAME" } },
    {"ICDPATIENT", new List<string> { "CFK_HR"} },
    {"INSCOMP", new List<string> { "CONTACT"} },
    {"OT", new List<string> { "MRN" } },
    {"OTITEMS", new List<string> { "MRN" } },
    {"PARTS", new List<string> { "NO", "NAME_" } },
    {"PATINS", new List<string> { "CPK_PATINS", "MRN" } },
    {"PNNAMES", new List<string> { "FNAME" } },
    {"PRONOTES", new List<string> { "MRN" } },
    {"SITES", new List<string> { "SITENAME" } },
    {"TICKCI", new List<string> { "MRN" } },
    {"TICKC", new List<string> { "MRN" } },
    {"PTDAYS", new List<string> { "MRN" } },
    {"POLICY", new List<string> { "CPK_POLICY", "MRN" } },
    {"INSVER", new List<string> { "MRN" } },
    {"LINEITEMS", new List<string> { "CFK_HR" } },
    {"ROSTER", new List<string> { "LNAME" } },
    {"CLAIMS", new List<string> { "CFK_HR" } },
    {"INVOICES", new List<string> { "CFK_HR" } },
    {"ICDMASTERLIST", new List<string> { "CODE" } },
    {"ICDORDER", new List<string> { "RANK" } },
    {"LOTLOG", new List<string> { "LOT" } },
    {"LABLOG", new List<string> { "LINK" } },
    {"LABELS", new List<string> { "LINK" } },
    {"LABELSHISTORY", new List<string> { "LINK" } },
    {"OTITEMSHISTORY", new List<string> { "NO" } },
    {"TICKETS", new List<string> { "MRN" } },
    {"VOIDRX", new List<string> { "CPK_VOIDRX", "USERNO" } },
    {"LINETRANS", new List<string> { "CFK_HR" } },
    {"LINEITEMSASSIGNED", new List<string> { "CFK_HR" } },
    {"CLIENT", new List<string> { "CPK_CLIENT", "NAME_" } }
            };
    }
}
