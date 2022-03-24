﻿namespace Dapper.Sharding
{
    internal class ClickHouseUnion : IUnion
    {
        public ClickHouseUnion(IDatabase db) : base(db)
        {

        }

        public override string GetSql()
        {
            if (take == 0)
            {
                return $"SELECT {returnFields} FROM ({sqlTable}) AS UTable{string.Concat(sqlWhere, sqlGroupBy, sqlHaving, sqlOrderBy)}";
            }
            else
            {
                return $"SELECT {returnFields} FROM ({sqlTable}) AS UTable{string.Concat(sqlWhere, sqlGroupBy, sqlHaving, sqlOrderBy)} LIMIT {skip},{take}";
            }
        }

        public override string GetSqlCount()
        {
            return $"SELECT COUNT(1) FROM ({sqlTable}) AS UTable{string.Concat(sqlWhere, sqlGroupBy, sqlHaving)}";
        }
    }
}
