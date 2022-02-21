using MySqlConnector;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dapper.Sharding
{
    internal class MySqlDatabase : IDatabase
    {
        public MySqlDatabase(string name, MySqlClient client) : base(name, client)
        {
            ConnectionString = ConnectionStringBuilder.BuilderMySql(client.Config, name);
        }

        protected override ITable<T> CreateITable<T>(string name)
        {
            return new MySqlTable<T>(name, this);
        }

        public override string ConnectionString { get; }

        public override IDbConnection GetConn()
        {
            var conn = new MySqlConnection(ConnectionString);
            if (conn.State == ConnectionState.Closed)
            {
                try
                {
                    conn.Open();
                }
                catch (Exception ex)
                {
                    conn.Dispose();
                    throw ex;
                }
            }
            return conn;
        }

        public override async Task<IDbConnection> GetConnAsync()
        {
            var conn = new MySqlConnection(ConnectionString);
            if (conn.State == ConnectionState.Closed)
            {
                try
                {
                    await conn.OpenAsync();
                }
                catch (Exception ex)
                {
                    await conn.DisposeAsync();
                    throw ex;
                }
            }
            return conn;
        }

        public override void DropTable(string name)
        {
            Execute($"DROP TABLE IF EXISTS `{name}`");
            TableCache.TryRemove(name, out _);
        }

        public override void TruncateTable(string name)
        {
            Execute($"TRUNCATE TABLE `{name}`");
        }

        public override IEnumerable<string> GetTableList()
        {
            return Query<string>("SHOW TABLES");
        }

        public override IEnumerable<string> GetTableColumnList(string name)
        {
            return Query<string>($"SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='{Name}' AND TABLE_NAME='{name}'");
        }

        public override bool ExistsTable(string name)
        {
            return !string.IsNullOrEmpty(QueryFirstOrDefault<string>($"SHOW TABLES LIKE '{name}'"));
        }

        public override string GetTableScript<T>(string name)
        {
            var tableEntity = ClassToTableEntityUtils.Get<T>(Client.DbType);
            var sb = new StringBuilder();
            sb.Append($"CREATE TABLE IF NOT EXISTS `{name}` (");
            foreach (var item in tableEntity.ColumnList)
            {
                sb.Append($"`{item.Name}` {item.DbType}");
                if (tableEntity.PrimaryKey.ToLower() == item.Name.ToLower())
                {
                    if (tableEntity.IsIdentity)
                    {
                        sb.Append(" AUTO_INCREMENT");
                    }
                    sb.Append(" PRIMARY KEY");
                }
                else
                {
#if CORE6
                    if (item.CsType.IsValueType && item.CsType != typeof(DateTime) && item.CsType != typeof(DateTimeOffset) && item.CsType!=typeof(DateOnly) && item.CsType != typeof(TimeOnly))
#else
                    if (item.CsType.IsValueType && item.CsType != typeof(DateTime) && item.CsType != typeof(DateTimeOffset))
#endif

                    {
                        sb.Append(" DEFAULT 0");
                    }
                }
                sb.Append($" COMMENT '{item.Comment}'");
                if (item != tableEntity.ColumnList.Last())
                {
                    sb.Append(",");
                }
            }

            if (tableEntity.IndexList != null && tableEntity.IndexList.Count > 0)
            {
                sb.Append(",");
                foreach (var ix in tableEntity.IndexList)
                {
                    if (ix.Type == IndexType.Normal)
                    {
                        sb.Append("KEY");
                    }
                    if (ix.Type == IndexType.Unique)
                    {
                        sb.Append("UNIQUE KEY");
                    }
                    if (ix.Type == IndexType.FullText)
                    {
                        sb.Append("FULLTEXT KEY");
                    }
                    if (ix.Type == IndexType.Spatial)
                    {
                        sb.Append("SPATIAL KEY");
                    }
                    sb.Append($" `{ix.Name}` ({ix.Columns})");
                    if (ix != tableEntity.IndexList.Last())
                    {
                        sb.Append(",");
                    }
                }
            }

            if (tableEntity.Engine == null)
            {
                tableEntity.Engine = "";
            }
            var engineList = new string[] 
            { 
                "innodb", "myisam", "xtradb", "aria", "tokudb", 
                "myrocks", "spider", "columnstore", "memory" ,"archive",
                "connect","csv","federatedx","cassandrase","sphinxse","mroonga",
                "sequence","blackhole","oqgraph"
            };
            var engine = engineList.FirstOrDefault(f => f == tableEntity.Engine.ToLower());
            if (engine == null)
            {
                engine = "InnoDB";
            }
            sb.Append($") ENGINE={engine} DEFAULT CHARSET={Client.Charset} COMMENT '{tableEntity.Comment}'");
            return sb.ToString();
        }

        public override TableEntity GetTableEntityFromDatabase(string name, bool firstCharToUpper = false)
        {
            dynamic data = QueryFirstOrDefault($"SHOW TABLE STATUS LIKE '{name}'");
            var entity = new TableEntity();
            entity.PrimaryKey = "";
            if (data.Auto_increment != null)
            {
                entity.IsIdentity = (data.Auto_increment >= 1);
            }
            entity.Comment = data.Comment;
            var manager = GetTableManager(name);
            var indexList = manager.GetIndexEntityList();
            entity.IndexList = indexList;
            var ix = indexList.FirstOrDefault(f => f.Type == IndexType.PrimaryKey);
            if (ix != null)
            {
                if (firstCharToUpper)
                {
                    entity.PrimaryKey = ix.Columns.FirstCharToUpper();
                }
                else
                {
                    entity.PrimaryKey = ix.Columns;
                }
            }
            entity.ColumnList = manager.GetColumnEntityList(null, firstCharToUpper);

            var col = entity.ColumnList.FirstOrDefault(w => w.Name.ToLower() == entity.PrimaryKey.ToLower());
            if (col != null)
            {
                entity.PrimaryKeyType = col.CsType;
            }

            return entity;

        }

        public override ITableManager GetTableManager(string name)
        {
            return new MySqlTableManager(name, this);
        }

        public override void OptimizeTable(string name, bool final = false, bool deduplicate = false)
        {
            Execute($"optimize table {name}");
        }

        public override void OptimizeTable(string name, string partition, bool final = false, bool deduplicate = false)
        {
            throw new System.NotImplementedException();
        }

        public override void Vacuum()
        {
            throw new NotImplementedException();
        }
    }
}
