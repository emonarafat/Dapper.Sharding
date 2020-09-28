using System;
using System.Collections.Concurrent;
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

        }

        protected override ITable<T> GetITable<T>(string name)
        {
            return new MySqlTable<T>(name, this);
        }

        public override IDbConnection GetConn()
        {
            var conn = Client.GetConn();
            if (conn.Database != Name)
                conn.ChangeDatabase(Name);
            return conn;
        }

        public override async Task<IDbConnection> GetConnAsync()
        {
            var conn = await Client.GetConnAsync();
            if (conn.Database != Name)
                conn.ChangeDatabase(Name);
            return conn;
        }

        public override void SetCharset(string chartset)
        {
            using (var conn = GetConn())
            {
                conn.Execute($"ALTER DATABASE `{Name}` DEFAULT CHARACTER SET {chartset} COLLATE {chartset}_general_ci");
            }
        }

        public override void DropTable(string name)
        {
            using (var conn = GetConn())
            {
                conn.Execute($"DROP TABLE IF EXISTS `{name}`");
            }
            TableCache.TryRemove(name.ToLower(), out _);
        }

        public override void TruncateTable(string name)
        {
            using (var conn = GetConn())
            {
                conn.Execute($"TRUNCATE TABLE `{name}`");
            }
        }

        public override IEnumerable<string> GetTableList()
        {
            using (var conn = GetConn())
            {
                return conn.Query<string>("SHOW TABLES");
            }
        }

        public override IEnumerable<string> GetTableColumnList(string name)
        {
            using (var conn = GetConn())
            {
                return conn.Query<string>($"SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='{Name}' AND TABLE_NAME='{name}'");
            }
        }

        public override bool ExistsTable(string name)
        {
            using (var conn = GetConn())
            {
                return !string.IsNullOrEmpty(conn.QueryFirstOrDefault<string>($"SHOW TABLES LIKE '{name}'"));
            }
        }

        public override string GetTableScript<T>(string name)
        {
            var tableEntity = ClassToTableEntityUtils.Get<T>();
            var sb = new StringBuilder();
            sb.Append($"CREATE TABLE IF NOT EXISTS `{name}` (");
            foreach (var item in tableEntity.ColumnList)
            {
                sb.Append($"`{item.Name}` {item.DbType}");
                if (!string.IsNullOrEmpty(tableEntity.PrimaryKey))
                {
                    if (tableEntity.PrimaryKey.ToLower() == item.Name.ToLower())
                    {
                        if (tableEntity.IsIdentity)
                        {
                            sb.Append(" AUTO_INCREMENT");
                        }
                        sb.Append(" PRIMARY KEY");
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
            sb.Append($")DEFAULT CHARSET={Client.Charset} COMMENT '{tableEntity.Comment}'");

            return sb.ToString();
        }

        public override TableEntity GetTableEntityFromDatabase(string name)
        {
            dynamic data;
            using (var conn = GetConn())
            {
                data = conn.QueryFirstOrDefault($"SHOW TABLE STATUS LIKE '{name}'");
            }
            var entity = new TableEntity();
            if (data.Auto_increment != null)
            {
                entity.IsIdentity = (data.Auto_increment >= 1);
            }
            entity.Comment = data.Comment;
            var manager = GetTableManager((string)data.Name);
            var indexList = manager.GetIndexEntityList();
            entity.IndexList = indexList;
            var ix = indexList.FirstOrDefault(f => f.Type == IndexType.PrimaryKey);
            if (ix != null)
            {
                entity.PrimaryKey = ix.Columns.FirstCharToUpper();
            }
            entity.ColumnList = manager.GetColumnEntityList();

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

    }
}
