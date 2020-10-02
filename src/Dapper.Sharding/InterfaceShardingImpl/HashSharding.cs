namespace Dapper.Sharding
{
    internal class HashSharding<T> : ISharding<T> where T : class
    {
        public HashSharding(ITable<T>[] tableList, DistributedTransaction tran = null) : base(tableList, tran)
        {

        }

        public override ISharding<T> CreateTranSharding(DistributedTransaction tran)
        {
            return new HashSharding<T>(TableList, tran);
        }

        public override ITable<T> GetTableById(object id)
        {
            return TableList[ShardingUtils.Mod(id, TableList.Length)];
        }

        public override ITable<T> GetTableByModel(T model)
        {
            return TableList[ShardingUtils.Mod(model, SqlField.PrimaryKey, SqlField.PrimaryKeyType, TableList.Length)];
        }

    }
}
