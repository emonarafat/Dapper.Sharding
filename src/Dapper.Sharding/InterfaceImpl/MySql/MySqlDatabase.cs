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
            var table = ClassToTableEntityUtils.Get<T>(name);
            var sb = new StringBuilder();
            sb.Append($"CREATE TABLE IF NOT EXISTS `{table.Name}` (");
            foreach (var item in table.ColumnList)
            {
                sb.Append($"`{item.Name}` {item.DbType}");
                if (!string.IsNullOrEmpty(table.PrimaryKey))
                {
                    if (table.PrimaryKey.ToLower() == item.Name.ToLower())
                    {
                        if (table.IsIdentity)
                        {
                            sb.Append(" AUTO_INCREMENT");
                        }
                        sb.Append(" PRIMARY KEY");
                    }
                }
                sb.Append($" COMMENT '{item.Comment}'");
                if (item != table.ColumnList.Last())
                {
                    sb.Append(",");
                }
            }

            if (table.IndexList != null && table.IndexList.Count > 0)
            {
                sb.Append(",");
                foreach (var ix in table.IndexList)
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
                    if (ix != table.IndexList.Last())
                    {
                        sb.Append(",");
                    }
                }
            }
            sb.Append($")DEFAULT CHARSET={Client.Charset} COMMENT '{table.Comment}'");

            return sb.ToString();
        }

        public IEnumerable<dynamic> ShowTableStatus()
        {
            IEnumerable<dynamic> data = null;
            Using(conn =>
            {
                data = conn.Query("SHOW TABLE STATUS");
            });
            return data;
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
                        var tableEntity = ClassToTableEntityUtils.Get<T>(name);
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

        public ITableManager GetTableManager(string name)
        {
            return new MySqlTableManager(name, this);
        }

        public ITable<T> GetTable<T>(string name, IDbConnection conn, IDbTransaction tran = null, int? commandTimeout = null)
        {
            if (Client.AutoCreateTable)
            {
                if (!TableCache.Contains(name))
                {
                    lock (Locker.GetObject(name))
                    {
                        if (!TableCache.Contains(name))
                        {
                            CreateTable<T>(name);
                            TableCache.Add(name);
                        }
                    }
                }
            }
            return new MySqlTable<T>(name, conn, tran, commandTimeout);
        }

        public List<TableEntity> GetTableEnitys()
        {
            var list = new List<TableEntity>();
            var statusList = ShowTableStatus();
            foreach (var item in statusList)
            {
                var entity = new TableEntity();
                entity.Name = item.Name;
                if (item.Auto_increment != null)
                {
                    entity.IsIdentity = item.Auto_increment == 1 ? true : false;
                }
                entity.Comment = item.Comment;
                var manager = GetTableManager(entity.Name);
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





        #endregion
    }
}
