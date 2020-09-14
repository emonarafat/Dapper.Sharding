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
        public MySqlDatabase(string name, MySqlClient client)
        {
            Name = name;
            Client = client;
            Locker = new LockManager();
            TableCache = new ConcurrentDictionary<string, object>();
        }

        public string Name { get; }

        public IClient Client { get; }

        public LockManager Locker { get; }

        public ConcurrentDictionary<string, object> TableCache { get; }

        public IDbConnection GetConn()
        {
            var conn = Client.GetConn();
            if (conn.Database != Name)
                conn.ChangeDatabase(Name);
            return conn;
        }


        public async Task<IDbConnection> GetConnAsync()
        {
            var conn = await Client.GetConnAsync();
            if (conn.Database != Name)
                conn.ChangeDatabase(Name);
            return conn;
        }

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

        public void CreateTable<T>(string name)
        {
            using (var conn = GetConn())
            {
                var script = ShowTableScript<T>(name);
                conn.Execute(script);
            }
        }

        public void DropTable(string name)
        {
            Using(conn =>
            {
                conn.Execute($"DROP TABLE IF EXISTS `{name}`");
            });
            TableCache.TryRemove(name.ToLower(), out _);
        }

        public void TruncateTable(string name)
        {
            Using(conn =>
            {
                conn.Execute($"TRUNCATE TABLE `{name}`");
            });

        }


        public IEnumerable<string> ShowTableList()
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

        public IEnumerable<dynamic> ShowTableStatusList()
        {
            using (var conn = GetConn())
            {
                return conn.Query("SHOW TABLE STATUS");
            }
        }

        public TableEntity StatusToTableEntity(dynamic data)
        {
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

        public ITableManager GetTableManager(string name)
        {
            return new MySqlTableManager(name, this);
        }

        public TableEntity GetTableEntityFromDatabase(string name)
        {
            return StatusToTableEntity(ShowTableStatus(name));
        }

        public List<TableEntity> GetTableEnityListFromDatabase()
        {
            var list = new List<TableEntity>();
            var statusList = ShowTableStatusList();
            foreach (var item in statusList)
            {
                list.Add(StatusToTableEntity(item));
            }
            return list;
        }

        public void GeneratorClassFile(string savePath, string tableList = "*", string nameSpace = "Model", string Suffix = "Table", bool partialClass = false)
        {
            this.CreateFiles(savePath, tableList, nameSpace, Suffix, partialClass);
        }

        public void CompareTableColumn<T>(string name, IEnumerable<string> dbColumns)
        {
            var tableEntity = ClassToTableEntityUtils.Get<T>();
            var manager = GetTableManager(name);

            foreach (var item in tableEntity.ColumnList)
            {
                if (!dbColumns.Any(a => a.ToLower().Equals(item.Name.ToLower())))
                {
                    manager.AddColumn(item.Name, item.CsType, item.Length, item.Comment);
                }
            }

            foreach (var item in dbColumns)
            {
                if (!tableEntity.ColumnList.Any(a => a.Name.ToLower().Equals(item.ToLower())))
                {
                    manager.DropColumn(item);
                }
            }
        }

        public ITable<T> GetTable<T>(string name)
        {
            var lowerName = name.ToLower();
            if (!TableCache.ContainsKey(lowerName))
            {
                lock (Locker.GetObject(lowerName))
                {
                    if (!TableCache.ContainsKey(lowerName))
                    {
                        if (Client.AutoCreateTable)
                        {
                            #region 创建、比对表

                            if (!ExistsTable(name))
                            {
                                CreateTable<T>(name);
                            }
                            else
                            {
                                if (Client.AutoCompareTableColumn)
                                {
                                    IEnumerable<string> dbColumns;

                                    using (var conn2 = GetConn())
                                    {
                                        dbColumns = conn2.Query<string>($"SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='{Name}' AND TABLE_NAME='{name}'");
                                    }
                                    CompareTableColumn<T>(name, dbColumns);
                                }
                            }

                            #endregion
                        }
                        TableCache.TryAdd(lowerName, new MySqlTable<T>(name, this));
                    }
                }
            }
            return (ITable<T>)TableCache[lowerName];
        }

    }
}
