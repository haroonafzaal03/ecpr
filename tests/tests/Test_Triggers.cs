using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace CPREnvoy
{
    public class Test_Triggers
    {
        private Config config;
        private readonly int nRows = 100;

        public Test_Triggers(Config config)
        {
            this.config = config;
        }

        public void runInsertTest()
        {
            runTests((table) => {
                Console.WriteLine("Inserting data to {0}", table.Key);
                insertTestData(table);
                verifyLogTableData(table);
            }, false);
        }

        public void runUpdateTest()
        {
            runTests((table) =>
            {
                Console.WriteLine("Updating data to {0}", table.Key);
            }, false);
        }

        public void runDeleteTest()
        {
            runTests((table) =>
            {
                Console.WriteLine("Deleting data to {0}", table.Key);
            }, false);
        }

        private void runTests(Action<KeyValuePair<string, List<string>>> callbackFN, bool cleanup = true)
        {
            if (cleanup)
                cleanupData();

            int i = 0;
            Task[] taskSQLs = new Task[TestQuery.TEST_QUERY.Count];

            foreach (var table in TestQuery.TEST_QUERY)
            {
                taskSQLs[i] = Task.Factory.StartNew(() =>
                {
                    callbackFN(table);
                });

                i += 1;
            }

            Task.WaitAll(taskSQLs);
        }

        public void cleanupData()
        {
            int i = 0;
            Task[] taskSQLs = new Task[TestQuery.TEST_QUERY.Count];

            foreach (var table in TestQuery.TEST_QUERY)
            {
                taskSQLs[i] = Task.Factory.StartNew(() =>
                {
                    Console.WriteLine("Deleting data on {0}", table.Key);
                    cleanupMainTable(table.Key);
                });

                i += 1;
            }

            Task.WaitAll(taskSQLs);
        }

        private void cleanupMainTable(string tableName)
        {
            TestDB.runSQLNoneQuery(config, String.Format("DELETE FROM {0}", tableName));
            TestDB.runSQLNoneQuery(config, String.Format("DELETE FROM ecpr_{0}_logs", tableName));
        }

        private void verifyLogTableData(KeyValuePair<string, List<string>> table)
        {
            var rows = TestDB.runSQLQuery(config, String.Format("SELECT COUNT(*) AS COUNT FROM ecpr_{0}_logs", table.Key));
            var count = rows.First();

            Debug.Assert((int)count.Value == nRows);          
        }

        private void insertTestData(KeyValuePair<string, List<string>> table)
        {
            string insertQueryTpl = "INSERT INTO {0} ({1}) VALUES({2})";
            string[] columns = table.Value.Select(x => "@" + x).ToArray();

            string insertQuery = String.Format(insertQueryTpl,
                table.Key, String.Join(",", table.Value.ToArray()),
                String.Join(",", columns)
                );

            for (int i = 1; i <= nRows; i++)
            {
                Dictionary<string, object> data = new Dictionary<string, object>();
                foreach (string column in columns)
                {
                    data.Add(column, i);
                }

                TestDB.runSQLNoneQuery(config, insertQuery, data);
            }
        }
    }
}
