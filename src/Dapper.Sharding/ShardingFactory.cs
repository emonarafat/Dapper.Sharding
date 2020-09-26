﻿using System.Collections.Generic;

namespace Dapper.Sharding
{
    public class ShardingFactory
    {
        public static void SetSnowFlakeWorker(long workerId, long datacenterId)
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

        public static DistributedTran CreateDistributedTran()
        {
            return new DistributedTran();
        }

        public static ReadWirteClient CreateReadWriteClient(IClient writeClient, params IClient[] readClientList)
        {
            return new ReadWirteClient(writeClient, readClientList);
        }

        public static ShardingQuery<T> CreateShardingQuery<T>(params ITable<T>[] tableList)
        {
            return new ShardingQuery<T>(tableList);
        }

        public static ISharding<T> CreateShardingAuto<T>(params ITable<T>[] tableList)
        {
            return new AutoSharding<T>(tableList);
        }

        public static ISharding<T> CreateShardingHash<T>(params ITable<T>[] tableList)
        {
            return new HashSharding<T>(tableList);
        }

        public static ISharding<T> CreateShardingRange<T>(Dictionary<long, ITable<T>> dict)
        {
            return new RangeSharding<T>(dict);
        }

        public static ReadWirteSharding<T> CreateReadWirteSharding<T>(ISharding<T> write, params ISharding<T>[] readList)
        {
            return new ReadWirteSharding<T>(write, readList);
        }

        public static string NextObjectId()
        {
            return ObjectId.GenerateNewIdAsString();
        }

        public static long NextSnowFlakeId()
        {
            return SnowflakeId.GenerateNewId();
        }
    }
}
