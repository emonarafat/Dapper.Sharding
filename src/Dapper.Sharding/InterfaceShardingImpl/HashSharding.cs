using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Dapper.Sharding
{
    internal class HashSharding<T> : ISharding<T> where T : class
    {
        public HashSharding(ITable<T>[] tableList) : base(tableList)
        {

        }

        public override ITable<T> GetTableById(object id)
        {
            return TableList[ShardingUtils.Mod(id, TableList.Length)];
        }

        public override ITable<T> GetTableByModel(T model)
        {
            return TableList[ShardingUtils.Mod(model, KeyName, KeyType, TableList.Length)];
        }

        public override ITable<T> GetTableByModelAndInitId(T model)
        {
            return TableList[ShardingUtils.ModAndInitId(model, KeyName, KeyType, TableList.Length)];
        }

    }
}
