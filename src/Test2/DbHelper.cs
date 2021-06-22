using Dapper.Sharding;

namespace Test2
{
    public class DbHelper
    {
        public static IClient Client = ShardingFactory.CreateClient(DataBaseType.Postgresql, new DataBaseConfig { Server = "127.0.0.1", UserId = "postgres", Password = "123", MinPoolSize = 1, MaxPoolSize = 2 });
        public static IClient Client2 = ShardingFactory.CreateClient(DataBaseType.Postgresql, new DataBaseConfig { Server = "127.0.0.1", UserId = "postgres", Password = "123" });

        //public static IClient ClientHouse = ShardingFactory.CreateClient(DataBaseType.ClickHouse, new DataBaseConfig { Server = "192.168.0.200" });
        public static IClient ClientHouse = ShardingFactory.CreateClient(DataBaseType.ClickHouse, new DataBaseConfig { Server = "192.168.238.129" });
        public static IDatabase Db
        {
            get
            {
                return ClientHouse.GetDatabase("test");
            }
        }

        public static ITable<People> peopleTable
        {
            get
            {
                return Db.GetTable<People>("people");
            }
        }

        public static ITable<Student> studentTable
        {
            get
            {
                return Db.GetTable<Student>("student");
            }
        }

        public static ITable<Teacher> teacherTable
        {
            get
            {
                return Db.GetTable<Teacher>("teacher");
            }
        }

        public static ITable<TreeTable> treeTable
        {
            get
            {
                return Db.GetTable<TreeTable>("tree");
            }
        }
    }
}
