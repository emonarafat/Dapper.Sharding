namespace Dapper.Sharding
{
    public class ShardingFactory
    {
        public static void SetSnowflakeWorker(long workerId, long datacenterId)
        {
            SnowflakeId.worker = new IdWorker(workerId, datacenterId);
        }

        public static IClient CreateClient(DataBaseType dbType, string connectionString)
        {
            switch (dbType)
            {
                case DataBaseType.MySql:
                    return new MySqlClient(connectionString);
                case DataBaseType.SqlServer2008:
                    return new SqlServerClient(connectionString, DataBaseType.SqlServer2008);
                case DataBaseType.SqlServer2012:
                    return new SqlServerClient(connectionString, DataBaseType.SqlServer2012);
                case DataBaseType.Sqlite:
                    return new SQLiteClient(connectionString);
                case DataBaseType.Postgresql:
                    return new PostgreClient(connectionString);
                case DataBaseType.Oracel:
                    return new OracleClient(connectionString);
            }
            return null;
        }

        public static ReadWirteClient CreateReadWriteClient(IClient writeClient, params IClient[] readClientList)
        {
            return new ReadWirteClient(writeClient, readClientList);
        }

        public static ISharding<T> CreateShardingAuto<T>(params ITable<T>[] tableList)
        {
            return new AutoSharding<T>(tableList);
        }

        public static ShardingQuery<T> CreateShardingQuery<T>(params ITable<T>[] tableList)
        {
            return new ShardingQuery<T>(tableList);
        }
    }
}
