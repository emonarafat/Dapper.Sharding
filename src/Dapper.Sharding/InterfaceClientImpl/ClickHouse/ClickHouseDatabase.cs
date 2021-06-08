using ClickHouse.Ado;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dapper.Sharding
{
    internal class ClickHouseDatabase : IDatabase
    {
        public ClickHouseDatabase(string name, ClickHouseClient client) : base(name, client)
        {
            ConnectionString = ConnectionStringBuilder.BuilderClickHouse(client.Config, name);
        }

        public override string ConnectionString { get; }

        public override void DropTable(string name)
        {
            Execute($"DROP TABLE IF EXISTS {name}");
            TableCache.TryRemove(name, out _);
        }

        public override bool ExistsTable(string name)
        {
            return !string.IsNullOrEmpty(QueryFirstOrDefault<string>($"SHOW TABLES LIKE '{name}'"));
        }

        public override IDbConnection GetConn()
        {
            var conn = new ClickHouseConnection(ConnectionString);
            if (conn.State != ConnectionState.Open)
                conn.Open();
            return conn;
        }

        public override Task<IDbConnection> GetConnAsync()
        {
            throw new NotImplementedException();
        }

        public override IEnumerable<string> GetTableColumnList(string name)
        {
            return Query($"DESCRIBE TABLE {name}").Select(s => (string)s.name);
        }

        public override TableEntity GetTableEntityFromDatabase(string name)
        {
            var entity = new TableEntity();
            var manager = GetTableManager(name);
            entity.ColumnList = manager.GetColumnEntityList();
            return entity;
        }

        public override IEnumerable<string> GetTableList()
        {
            return Query<string>("SHOW TABLES");
        }

        public override ITableManager GetTableManager(string name)
        {
            return new ClickHouseTableManager(name, this);
        }

        public override string GetTableScript<T>(string name)
        {
            var tableEntity = ClassToTableEntityUtils.Get<T>(Client.DbType);
            var sb = new StringBuilder();
            if (string.IsNullOrEmpty(tableEntity.Cluster))
            {
                sb.Append($"CREATE TABLE IF NOT EXISTS {name} (");
            }
            else
            {
                sb.Append($"CREATE TABLE IF NOT EXISTS {name} ON CLUSTER {tableEntity.Cluster} (");
            }
            foreach (var item in tableEntity.ColumnList)
            {
                sb.Append($"{item.Name} {item.DbType}");
                sb.Append($" COMMENT '{item.Comment}'");
                if (item != tableEntity.ColumnList.Last())
                {
                    sb.Append(',');
                }
            }
            sb.Append($")ENGINE={tableEntity.Engine}");
            return sb.ToString();
        }

        public override void OptimizeTable(string name, bool final = false, bool deduplicate = false)
        {
            var sql = $"OPTIMIZE TABLE {name}";
            if (final)
            {
                sql += " FINAL";
            }
            if (deduplicate)
            {
                sql += " DEDUPLICATE";
            }
            Execute(sql);
        }

        public override void OptimizeTable(string name, string partition, bool final = false, bool deduplicate = false)
        {
            var sql = $"OPTIMIZE TABLE {name} PARTITION {partition}";
            if (final)
            {
                sql += " FINAL";
            }
            if (deduplicate)
            {
                sql += " DEDUPLICATE";
            }
            Execute(sql);
        }

        public override void SetCharset(string chartset)
        {
            throw new NotImplementedException();
        }

        public override void TruncateTable(string name)
        {
            throw new NotImplementedException();
        }

        protected override ITable<T> CreateITable<T>(string name)
        {
            return new ClickHouseTable<T>(name, this);
        }
    }
}
