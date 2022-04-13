using System;
using System.Collections.Generic;
using Z.BulkOperations;
using Z.Dapper.Plus;

namespace Dapper.Sharding
{
    public class ShardingFactory
    {
#if CORE6

        public static DbTypeDateOnly DateOnlyFormat { get; set; } = DbTypeDateOnly.Date;
        public static DbTypeTimeOnly TimeOnlyFormat { get; set; } = DbTypeTimeOnly.TimeSpan;//only mysql and pgsql,other use time
        static ShardingFactory()
        {
            SqlMapper.AddTypeHandler(new DateOnlyTypeHandler());
            SqlMapper.AddTypeHandler(new TimeOnlyTypeHandler());
            DapperPlusManager.AddValueConverter(typeof(DateOnly), new DateOnlyTypeHandlerZ());
            DapperPlusManager.AddValueConverter(typeof(TimeOnly), new TimeOnlyTypeHandlerZ());
        }
#endif

        public static bool ClickHouseFixedString { get; set; } = false;

        public static void SetSnowFlakeWorker(long workerId, long datacenterId, long seqLength = 0)
        {
            SnowflakeId.worker = new IdWorker(workerId, datacenterId, seqLength);
        }

        public static void SetLongIdWorker(ushort workerId = 0, byte workLength = 6, byte seqLength = 6)
        {
            //https://gitee.com/yitter/idgenerator
            //https://github.com/yitter/IdGenerator
            var opt = new IdGeneratorOptions();
            if (workLength != 6)
            {
                opt.WorkerIdBitLength = workLength; //默认值6，取值范围 [1, 15]（要求：序列数位长+机器码位长不超过22）
            }
            if (workerId != 0) //当workLength等于6,workerId最大值63
            {
                opt.WorkerId = workerId; //最大值 2 ^ WorkerIdBitLength - 1
            }
            if (seqLength != 6) //默认6支持10万并发,10才能支持50-200万并发,取值范围[3,21]
            {
                opt.SeqBitLength = seqLength;
            }
            IdHelper.IdGenInstance = new DefaultIdGenerator(opt);
        }

        public static IClient CreateClient(DataBaseType dbType, DataBaseConfig config)
        {
            switch (dbType)
            {
                case DataBaseType.MySql:
                    return new MySqlClient(config);
                case DataBaseType.SqlServer2005:
                    return new SqlServerClient(config, DataBaseType.SqlServer2005);
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

        public static ShardingQuery<T> CreateShardingQuery<T>(params ITable<T>[] tableList) where T : class
        {
            return new ShardingQuery<T>(tableList);
        }

        public static ShardingQueryDb CreateShardingQueryDb(params IDatabase[] dbList)
        {
            return new ShardingQueryDb(dbList);
        }

        public static ShardingQueryClient CreateShardingQueryClient(params IClient[] clientList)
        {
            return new ShardingQueryClient(clientList);
        }

        public static ISharding<T> CreateShardingHash<T>(params ITable<T>[] tableList) where T : class
        {
            return new HashSharding<T>(tableList);
        }

        public static ISharding<T> CreateShardingRange<T>(Dictionary<long, ITable<T>> dict) where T : class
        {
            return new RangeSharding<T>(dict);
        }

        public static ISharding<T> CreateShardingAuto<T>(params ITable<T>[] tableList) where T : class
        {
            return new AutoSharding<T>(tableList);
        }

        public static ReadWirteSharding<T> CreateReadWirteSharding<T>(ISharding<T> write, params ISharding<T>[] readList) where T : class
        {
            return new ReadWirteSharding<T>(write, readList);
        }

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

        public static long NextLongId()
        {
            return IdHelper.IdGenInstance.NewLong();
        }

        public static string NextLongIdAsString()
        {
            return NextLongId().ToString();
        }

        public static void AddTypeHandler(Type type, SqlMapper.ITypeHandler handler, IBulkValueConverter handlerz)
        {
            TypeHandlerCache.Add(type, () =>
            {
                SqlMapper.AddTypeHandler(type, handler);
                DapperPlusManager.AddValueConverter(type, handlerz);
            });
        }
    }
}
