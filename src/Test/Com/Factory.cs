using Dapper.Sharding;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Test
{
    public class Factory
    {
        //Must singleton mode(必须是单例模式)
        public static IClient Client = ClientFactory.CreateClient(DataBaseType.MySql, "server=127.0.0.1;user=root");

        public static IDatabase Db
        {
            get
            {
                return Client.GetDatabase("test");
            }
        }

        public static ITableManager TableManager
        {
            get
            {
                return Db.GetTableManager("People");
            }
        }

    }
}
