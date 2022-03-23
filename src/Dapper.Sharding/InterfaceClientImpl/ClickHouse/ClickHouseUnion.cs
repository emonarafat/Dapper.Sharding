namespace Dapper.Sharding
{
    internal class ClickHouseUnion : IUnion
    {
        public ClickHouseUnion(IDatabase db) : base(db)
        {

        }

        internal override void Build()
        {
            if (take == 0)
            {
                _sql = $"SELECT {returnFields} FROM ({_sql}) AS UTable{string.Concat(sqlWhere, sqlGroupBy, sqlHaving, sqlOrderBy)}";
            }
            else
            {
                _sql = $"SELECT {returnFields} FROM ({_sql}) AS UTable{string.Concat(sqlWhere, sqlGroupBy, sqlHaving, sqlOrderBy)} LIMIT {skip},{take}";
            }
        }

        internal override void BuildCount()
        {
            _sqlCount = $"SELECT COUNT(1) FROM ({_sql}) AS UTable{string.Concat(sqlWhere, sqlGroupBy, sqlHaving)}";
        }
    }
}
