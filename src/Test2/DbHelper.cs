using Dapper.Sharding;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Test2
{
    public class DbHelper
    {
        public static IClient Client = ShardingFactory.CreateClient(DataBaseType.Postgresql, new DataBaseConfig { Server = "127.0.0.1", UserId = "postgres", Password = "123", MinPoolSize = 1, MaxPoolSize = 2 });
        public static IClient Client2 = ShardingFactory.CreateClient(DataBaseType.Postgresql, new DataBaseConfig { Server = "127.0.0.1", UserId = "postgres", Password = "123" });

        public static IClient ClientHouse = ShardingFactory.CreateClient(DataBaseType.ClickHouse, new DataBaseConfig { Server = "192.168.0.200" });
    }
}
