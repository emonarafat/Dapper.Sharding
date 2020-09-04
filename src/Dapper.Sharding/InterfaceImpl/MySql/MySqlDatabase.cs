using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;

namespace Dapper.Sharding
{
    internal class MySqlDatabase : IDatabase
    {

        public string Name { get; }

        public IClient Client { get; }

        public LockManager Locker { get; } = new LockManager();

        public HashSet<string> TableCache { get; } = new HashSet<string>();

        public MySqlDatabase(string name, MySqlClient client)
        {
            Name = name;
            Client = client;
        }

        public IDbConnection GetConn()
        {
            var conn = Client.GetConn();
            if (conn.Database != Name)
                conn.ChangeDatabase(Name);
            return conn;
        }

        #region interface method

        public void Using(Action<IDbConnection> action)
        {
            using (var conn = GetConn())
            {
                action(conn);
            }
        }

        public void UsingTran(Action<IDbConnection, IDbTransaction> action)
        {
            using (var conn = GetConn())
            {
                using (var tran = conn.BeginTransaction())
                {
                    action(conn, tran);
                }
            }
        }

        public void SetCharset(string chartset)
        {
            Using(conn =>
            {
                conn.Execute($"ALTER DATABASE `{Name}` DEFAULT CHARACTER SET {chartset} COLLATE {chartset}_general_ci");
            });
        }

        public void DropTable(string name)
        {
            Using(conn =>
            {
                conn.Execute($"DROP TABLE IF EXISTS `{name}`");
            });
            TableCache.Remove(name);
        }

        public IEnumerable<string> ShowTables()
        {
            using (var conn = GetConn())
            {
                return conn.Query<string>("SHOW TABLES");
            }
        }

        public bool ExistsTable(string name)
        {
            using (var conn = GetConn())
            {
                return !string.IsNullOrEmpty(conn.QueryFirstOrDefault<string>($"SHOW TABLES LIKE '{name}'"));
            }
        }

        public string ShowTableScript<T>(string name)
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

        public dynamic ShowTableStatus(string name)
        {
            using (var conn = GetConn())
            {
                return conn.QueryFirstOrDefault($"SHOW TABLE STATUS LIKE '{name}'");
            }
        }

        public IEnumerable<dynamic> ShowTablesStatus()
        {
            using (var conn = GetConn())
            {
                return conn.Query("SHOW TABLE STATUS");
            }
        }

        public ITableManager GetTableManager(string name, IDbConnection conn = null, IDbTransaction tran = null, int? commandTimeout = null)
        {
            return new MySqlTableManager(name, this, conn, tran, commandTimeout);
        }

        public TableEntity GetTableEntityFromDatabase(string name)
        {
            throw new NotImplementedException();
        }

        public List<TableEntity> GetTableEnitys()
        {
            var list = new List<TableEntity>();
            var statusList = ShowTablesStatus();
            foreach (var item in statusList)
            {
                var entity = new TableEntity();
                if (item.Auto_increment != null)
                {
                    entity.IsIdentity = (item.Auto_increment == 1);
                }
                entity.Comment = item.Comment;
                var manager = GetTableManager((string)item.Name);
                var indexList = manager.GetIndexEntitys();
                entity.IndexList = indexList;
                var ix = indexList.FirstOrDefault(f => f.Type == IndexType.PrimaryKey);
                if (ix != null)
                {
                    entity.PrimaryKey = ix.Columns.FirstCharToUpper();
                }
                entity.ColumnList = manager.GetColumnEntitys();
                list.Add(entity);
            }
            return list;
        }

        public void CreateTable<T>(string name)
        {
            Using(conn =>
            {
                var obj = conn.QueryFirstOrDefault($"SHOW TABLES LIKE '{name}'");
                if (obj == null)
                    conn.Execute(ShowTableScript<T>(name));
                else
                {
                    if (Client.AutoCompareTableColumn)
                    {
                        var dbColumns = conn.Query<string>($"SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='{Name}' AND TABLE_NAME='{name}'");
                        var tableEntity = ClassToTableEntityUtils.Get<T>();
                        var manager = GetTableManager(name);

                        foreach (var item in dbColumns)
                        {
                            if (!tableEntity.ColumnList.Any(a => a.Name.ToLower() == item.ToLower()))
                            {
                                manager.DropColumn(item);
                            }
                        }
                        foreach (var item in tableEntity.ColumnList)
                        {
                            if (!dbColumns.Any(a => a.ToLower() == item.Name.ToLower()))
                            {
                                manager.AddColumn(item.Name, item.CsType, item.Length, item.Comment);
                            }
                        }
                    }
                }
            });
        }

        public ITable<T> GetTable<T>(string name, IDbConnection conn = null, IDbTransaction tran = null, int? commandTimeout = null)
        {
            var lowerName = name.ToLower();
            if (Client.AutoCreateTable)
            {
                if (!TableCache.Contains(lowerName))
                {
                    lock (Locker.GetObject(lowerName))
                    {
                        if (!TableCache.Contains(lowerName))
                        {
                            CreateTable<T>(name);
                            TableCache.Add(lowerName);
                        }
                    }
                }
            }
            return new MySqlTable<T>(name, this, conn, tran, commandTimeout);
        }

        #endregion
    }
}
