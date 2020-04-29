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
        static IDapperClient client = DapperFactory.CreateMySqlClient("server=127.0.0.1;uid=root;pwd=123456;Charset=utf8");

        public static string Database = "test";

        public static string Table = "ddd";

        public static IDapperClient GetClient()
        {
            return client;
        }

        public static IDapperDatabase GetDatabase()
        {
            return GetClient().GetDatabase(Database);
        }

        public static IDapperTableManager GetTableManager()
        {
            return GetDatabase().GetTableManager(Table);
        }

    }
}
