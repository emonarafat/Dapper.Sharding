using System.Collections.Generic;

namespace Dapper.Sharding
{
    public class ShardingFactory
    {
        public static void SetSnowFlakeWorker(long workerId, long datacenterId)
        {
            SnowflakeId.worker = new IdWorker(workerId, datacenterId);
        }

        public static IClient CreateClient(DataBaseType dbType, DataBaseConfig config)
        {
            switch (dbType)
            {
                case DataBaseType.MySql:
                    return new MySqlClient(config);
                case DataBaseType.SqlServer2008:
                    return new SqlServerClient(config, DataBaseType.SqlServer2008);
                case DataBaseType.SqlServer2012:
                    return new SqlServerClient(config, DataBaseType.SqlServer2012);
                case DataBaseType.Sqlite:
                    return new SQLiteClient(config);
                case DataBaseType.Postgresql:
                    return new PostgreClient(config);
                case DataBaseType.Oracle:
                    return new OracleClient(config);
                case DataBaseType.ClickHouse:
                    return new ClickHouseClient(config);
            }
            return null;
        }

        public static DistributedTransaction CreateDistributedTransaction()
        {
            return new DistributedTransaction();
        }

        public static ReadWirteClient CreateReadWriteClient(IClient writeClient, params IClient[] readClientList)
        {
            return new ReadWirteClient(writeClient, readClientList);
        }

        //public static ShardingQuery<T> CreateShardingQuery<T>(params ITable<T>[] tableList) where T : class
        //{
        //    return new ShardingQuery<T>(tableList);
        //}

        //public static ShardingQueryDb CreateShardingQueryDb(params IDatabase[] dbList)
        //{
        //    return new ShardingQueryDb(dbList);
        //}

        //public static ShardingQueryClient CreateShardingQueryClient(params IClient[] clientList)
        //{
        //    return new ShardingQueryClient(clientList);
        //}

        //public static ISharding<T> CreateShardingHash<T>(params ITable<T>[] tableList) where T : class
        //{
        //    return new HashSharding<T>(tableList);
        //}

        //public static ISharding<T> CreateShardingRange<T>(Dictionary<long, ITable<T>> dict) where T : class
        //{
        //    return new RangeSharding<T>(dict);
        //}

        //public static ISharding<T> CreateShardingAuto<T>(params ITable<T>[] tableList) where T : class
        //{
        //    return new AutoSharding<T>(tableList);
        //}

        //public static ReadWirteSharding<T> CreateReadWirteSharding<T>(ISharding<T> write, params ISharding<T>[] readList) where T : class
        //{
        //    return new ReadWirteSharding<T>(write, readList);
        //}

        public static string NextObjectId()
        {
            return ObjectId.GenerateNewIdAsString();
        }

        public static long NextSnowId()
        {
            return SnowflakeId.GenerateNewId();
        }

        public static string NextSnowIdAsString()
        {
            return NextSnowId().ToString();
        }
    }
}
