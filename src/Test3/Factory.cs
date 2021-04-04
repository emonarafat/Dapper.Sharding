using Dapper.Sharding;
using System.Collections.Generic;

namespace Test2
{
    public class Factory
    {
        //client must singleton mode(必须是单例模式)

        /*===mysql===*/
        //public static IClient Client = ShardingFactory.CreateClient(DataBaseType.MySql, new DataBaseConfig { Server = "127.0.0.1", UserId = "root", Password = "123", Port = 3306 });
        //public static IClient Client2 = ShardingFactory.CreateClient(DataBaseType.MySql, new DataBaseConfig { Server = "127.0.0.1", UserId = "root", Password = "123", Port = 3307 });


        /*===postgresql===*/
        public static IClient Client = ShardingFactory.CreateClient(DataBaseType.Postgresql, new DataBaseConfig { Server = "127.0.0.1", UserId = "postgres", Password = "123", MinPoolSize = 1, MaxPoolSize = 2 });
        public static IClient Client2 = ShardingFactory.CreateClient(DataBaseType.Postgresql, new DataBaseConfig { Server = "127.0.0.1", UserId = "postgres", Password = "123" });
    }
}
