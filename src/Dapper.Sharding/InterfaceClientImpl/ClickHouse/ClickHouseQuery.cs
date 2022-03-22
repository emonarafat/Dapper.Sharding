namespace Dapper.Sharding
{
    internal class ClickHouseQuery : IQuery
    {
        public ClickHouseQuery(IDatabase db) : base(db)
        {

        }

        internal override IQuery Add<T>(ITable<T> table, string asName = null)
        {
            primaryKey = table.SqlField.PrimaryKey;
            if (string.IsNullOrEmpty(asName))
            {
                sqlTable = $"{table.Name}";
                returnFields = table.SqlField.AllFields;
                sqlOrderBy = primaryKey;
            }
            else
            {
                sqlTable = $"{table.Name} AS {asName}";
                returnFields = $"{asName}.*";
                sqlOrderBy = $"{asName}.{primaryKey}";
            }
            return this;
        }

        public override IQuery InnerJoin<T>(ITable<T> table, string asName, string on)
        {
            sqlTable += $" INNER JOIN {table.Name} AS {asName} ON {on}";
            return this;
        }

        public override IQuery LeftJoin<T>(ITable<T> table, string asName, string on)
        {
            sqlTable += $" LEFT JOIN {table.Name} AS {asName} ON {on}";
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
                _sql = $"SELECT {returnFields} FROM {sqlTable} {sqlWhere} {sqlGroupBy} {sqlHaving} ORDER BY {sqlOrderBy} LIMIT {skip},{take}";
            }
        }

        internal override void BuildCount()
        {
            _sqlCount = $"SELECT COUNT(1) FROM {sqlTable} {sqlWhere} {sqlGroupBy} {sqlHaving}";
        }
    }
}
