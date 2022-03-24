namespace Dapper.Sharding
{
    internal class SqlServerUnion : IUnion
    {
        public SqlServerUnion(IDatabase db) : base(db)
        {
        }

        internal override void Build()
        {
            if (take == 0)
            {
                _sql = $"SELECT {returnFields} FROM ({sqlTable}) AS UTable{string.Concat(sqlWhere, sqlGroupBy, sqlHaving, sqlOrderBy)}";
            }
            else
            {
                if (skip == 0) //第一页,使用Top语句
                {
                    _sql = $"SELECT TOP ({take}) {returnFields} FROM ({sqlTable}) AS UTable{string.Concat(sqlWhere, sqlGroupBy, sqlHaving, sqlOrderBy)}";
                }
                else
                {
                    if (db.DbType == DataBaseType.SqlServer2012)
                    {
                        _sql = $"SELECT {returnFields} FROM ({sqlTable}) AS UTable{string.Concat(sqlWhere, sqlGroupBy, sqlHaving, sqlOrderBy)} offset {skip} rows fetch next {take} rows only";
                    }
                    else
                    {
                        //使用ROW_NUMBER()
                        _sql = $"WITH cte AS(SELECT ROW_NUMBER() OVER({sqlOrderBy.Trim()}) AS Row_Number,{returnFields} FROM ({sqlTable}) AS UTable{string.Concat(sqlWhere, sqlGroupBy, sqlHaving)}) SELECT {returnFields} FROM cte WHERE cte.Row_Number BETWEEN {skip + 1} AND {skip + take}";
                    }
                }

            }
        }

        internal override void BuildCount()
        {
            _sqlCount = $"SELECT COUNT(1) FROM ({sqlTable}) AS UTable{string.Concat(sqlWhere, sqlGroupBy, sqlHaving)}";
        }
    }
}
