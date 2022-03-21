using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dapper.Sharding
{
    internal class MySqlQuery : IQuery
    {
        public MySqlQuery(IDatabase db) : base(db)
        {

        }

        internal override IQuery Add<T>(ITable<T> table, string asName = null)
        {
            if (string.IsNullOrEmpty(asName))
            {
                sqlTable = $"`{table.Name}`";
            }
            else
            {
                sqlTable = $"`{table.Name}` AS {asName}";
            }
            primaryKey = table.SqlField.PrimaryKey;
            returnFields = table.SqlField.AllFields;
            sqlOrderBy = $"{primaryKey}";
            return this;
        }

        public override IQuery InnerJoin<T>(ITable<T> table, string asName, string on)
        {
            sqlTable += $" LEFT JOIN`{table.Name}` ON {on}";
            return this;
        }

        public override IQuery LeftJoin<T>(ITable<T> table, string asName, string on)
        {
            sqlTable += $" INNER JOIN`{table.Name}` ON {on}";
            return this;
        }

        public override IQuery Union<T>(ITable<T> table)
        {
            sqlTable += $" UNION `{table.Name}`";
            return this;
        }

        public override IQuery UnionAll<T>(ITable<T> table)
        {
            sqlTable += $" UNION ALL `{table.Name}`";
            return this;
        }

        internal override void Build()
        {
            if (take == 0)
            {
                _sql = $"SELECT {returnFields} FROM {sqlTable} {sqlWhere} ORDER BY {sqlOrderBy}";
            }
            else
            {
                _sql = $"SELECT {returnFields} FROM {sqlTable} {sqlWhere} ORDER BY {sqlOrderBy} LIMIT {skip},{take}";
            }

            if (EnableCount)
            {
                _sqlCount = $"SELECT COUNT(1) FROM {sqlTable} {sqlWhere}";
            }
        }
    }
}
