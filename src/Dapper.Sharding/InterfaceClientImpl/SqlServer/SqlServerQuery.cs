namespace Dapper.Sharding
{
    internal class SqlServerQuery : IQuery
    {
        public SqlServerQuery(IDatabase db) : base(db)
        {

        }

        internal override IQuery Add<T>(ITable<T> table, string asName = null)
        {
            primaryKey = table.SqlField.PrimaryKey;
            if (string.IsNullOrEmpty(asName))
            {
                sqlTable = $"[{table.Name}]";
                returnFields = table.SqlField.AllFields;
                sqlOrderBy = primaryKey;
            }
            else
            {
                sqlTable = $"[{table.Name}] AS {asName}";
                returnFields = $"{asName}.*";
                sqlOrderBy = $"{asName}.{primaryKey}";
            }
            return this;
        }

        public override IQuery InnerJoin<T>(ITable<T> table, string asName, string on)
        {
            sqlTable += $" INNER JOIN [{table.Name}] AS {asName} ON {on}";
            return this;
        }

        public override IQuery LeftJoin<T>(ITable<T> table, string asName, string on)
        {
            sqlTable += $" LEFT JOIN [{table.Name}] AS {asName} ON {on}";
            return this;
        }

        internal override void Build()
        {
            if (take == 0)
            {
                _sql = $"SELECT {returnFields} FROM {sqlTable} {sqlWhere} {sqlGroupBy} {sqlHaving} ORDER BY {sqlOrderBy}";
            }
            else
            {
                if (db.DbType == DataBaseType.SqlServer2012)
                {
                    _sql = $"SELECT {returnFields} FROM {sqlTable} {sqlWhere} {sqlGroupBy} {sqlHaving} ORDER BY {sqlOrderBy} offset {skip} rows fetch next {take} rows only";
                }
                else
                {
                    if (skip == 0) //第一页,使用Top语句
                    {
                        _sql = $"SELECT TOP ({take}) {returnFields} FROM {sqlTable} {sqlWhere} {sqlGroupBy} {sqlHaving} ORDER BY {sqlOrderBy}";
                    }
                    else //使用ROW_NUMBER()
                    {
                        _sql = $"WITH cte AS(SELECT ROW_NUMBER() OVER(ORDER BY {sqlOrderBy}) AS Row_Number,{returnFields} FROM {sqlTable} {sqlWhere} {sqlGroupBy} {sqlHaving}) SELECT * FROM cte WHERE cte.Row_Number BETWEEN {skip + 1} AND {skip + take}";
                    }
                }
            }
        }

        internal override void BuildCount()
        {
            _sqlCount = $"SELECT COUNT(1) FROM {sqlTable} {sqlWhere} {sqlGroupBy} {sqlHaving}";
        }
    }
}
