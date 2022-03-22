using System;

namespace Dapper.Sharding
{
    internal class OracleQuery : IQuery
    {
        public OracleQuery(IDatabase db) : base(db)
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
                _sql = $"SELECT * FROM(SELECT AA.*,rownum rn FROM(SELECT {returnFields} FROM {sqlTable} {sqlWhere} {sqlGroupBy} {sqlHaving} ORDER BY {sqlOrderBy}) AA WHERE rownum<={skip + take}) BB WHERE rn>={skip + 1}";
            }
        }

        internal override void BuildCount()
        {
            _sqlCount = $"SELECT COUNT(1) FROM {sqlTable} {sqlWhere} {sqlGroupBy} {sqlHaving}";
        }
    }
}
