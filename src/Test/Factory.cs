using Dapper.Sharding;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Test
{
    class Factory
    {
        public static IClient Client = DapperFactory.CreateMySqlClient("server=127.0.0.1;uid=root;pwd=123456;Charset=utf8");

        public static string Database = "test";

        public static string Table = "ddd";

        public static IDatabase GetDatabase()
        {
            return Client.GetDatabase(Database);
        }

        public static ITableManager GetTableManager()
        {
            return GetDatabase().GetTableManager(Table);
        }

    }
}
