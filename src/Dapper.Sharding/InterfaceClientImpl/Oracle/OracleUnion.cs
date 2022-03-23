namespace Dapper.Sharding
{
    internal class OracleUnion : IUnion
    {
        public OracleUnion(IDatabase db) : base(db)
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
                _sql = $"SELECT * FROM(SELECT AA.*,rownum rn FROM(SELECT {returnFields} FROM ({_sql}) AS UTable{string.Concat(sqlWhere, sqlGroupBy, sqlHaving, sqlOrderBy)}) AA WHERE rownum<={skip + take}) BB WHERE rn>={skip + 1}";
            }
        }

        internal override void BuildCount()
        {
            _sqlCount = $"SELECT COUNT(1) FROM ({_sql}) AS UTable{string.Concat(sqlWhere, sqlGroupBy, sqlHaving)}";
        }
    }
}
