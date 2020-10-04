using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SQLite;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dapper.Sharding
{
    internal class SQLiteDatabase : IDatabase
    {
        public SQLiteDatabase(string name, SQLiteClient client) : base(name, client)
        {
            ConnectionString = $"data source={Path.Combine(client.Config.Server, name)}";
        }

        public override string ConnectionString { get; }


        public override void DropTable(string name)
        {
            using (var conn = GetConn())
            {
                conn.Execute($"DROP TABLE {name}");
            }
        }

        public override bool ExistsTable(string name)
        {
            using (var conn = GetConn())
            {
                return conn.ExecuteScalar<int>($"SELECT COUNT(1) FROM sqlite_master WHERE name='{name}'") > 0;
            }
        }

        public override IDbConnection GetConn()
        {
            var conn = new SQLiteConnection(ConnectionString);
            if (conn.State != ConnectionState.Open)
                conn.Open();
            return conn;
        }

        public override async Task<IDbConnection> GetConnAsync()
        {
            var conn = new SQLiteConnection(ConnectionString);
            if (conn.State != ConnectionState.Open)
                await conn.OpenAsync();
            return conn;
        }

        public override IEnumerable<string> GetTableColumnList(string name)
        {
            IEnumerable<dynamic> data;
            using (var conn = GetConn())
            {
                data = conn.Query($"pragma table_info('{name}')");//字段信息
            }
            return data.Select(s => s.name as string);
        }

        public override TableEntity GetTableEntityFromDatabase(string name)
        {
            dynamic dynamicTable;
            IEnumerable<dynamic> dynamicColums;
            using (var conn = GetConn())
            {
                dynamicTable = conn.QueryFirst($"SELECT * FROM sqlite_master where type='table' and tbl_name='{name}'");
                dynamicColums = conn.Query($"pragma table_info('{name}')");
            }
            var entity = new TableEntity();
            var tablesql = ((string)dynamicTable.sql).ToUpper();
            if (tablesql.Contains("AUTOINCREMENT"))
            {
                entity.IsIdentity = true;
            }
            var row = dynamicColums.FirstOrDefault(f => f.pk == 1);
            if (row != null)
            {
                entity.PrimaryKey = ((string)row.name).FirstCharToUpper();
            }
            var manager = GetTableManager(name);
            var indexList = manager.GetIndexEntityList();
            entity.IndexList = indexList;
            entity.ColumnList = manager.GetColumnEntityList();
            var col = entity.ColumnList.FirstOrDefault(w => w.Name.ToLower() == entity.PrimaryKey.ToLower());
            if (col != null)
            {
                entity.PrimaryKeyType = col.CsType;
            }
            return entity;
        }

        public override IEnumerable<string> GetTableList()
        {
            using (var conn = GetConn())
            {
                return conn.Query<string>("select name from sqlite_master where type='table' and name!='sqlite_sequence'");
            }
        }

        public override ITableManager GetTableManager(string name)
        {
            return new SQLiteTableManager(name, this);
        }

        public override string GetTableScript<T>(string name)
        {
            var tableEntity = ClassToTableEntityUtils.Get<T>(Client.DbType);
            var sb = new StringBuilder();
            sb.Append($"CREATE TABLE IF NOT EXISTS [{name}] (");
            foreach (var item in tableEntity.ColumnList)
            {
                if (tableEntity.PrimaryKey.ToLower() == item.Name.ToLower())
                {
                    if (tableEntity.IsIdentity)
                    {
                        sb.Append($"[{item.Name}] INTEGER PRIMARY KEY AUTOINCREMENT");
                    }
                    else
                    {
                        sb.Append($"[{item.Name}] {item.DbType} PRIMARY KEY");
                    }
                }
                else
                {
                    sb.Append($"[{item.Name}] {item.DbType}");
                }

                if (item != tableEntity.ColumnList.Last())
                {
                    sb.Append(",");
                }
            }
            sb.Append(")");

            if (tableEntity.IndexList != null && tableEntity.IndexList.Count > 0)
            {
                sb.Append(";");
                foreach (var ix in tableEntity.IndexList)
                {
                    if (ix.Type == IndexType.Normal)
                    {
                        sb.Append("CREATE INDEX");
                    }
                    if (ix.Type == IndexType.Unique)
                    {
                        sb.Append("CREATE UNIQUE INDEX");
                    }
                    sb.Append($" {name}_{ix.Name} ON [{name}] ({ix.Columns})");
                    if (ix != tableEntity.IndexList.Last())
                    {
                        sb.Append(";");
                    }
                }
            }
            return sb.ToString();
        }

        public override void SetCharset(string chartset)
        {
            throw new NotImplementedException();
        }

        public override void TruncateTable(string name)
        {
            using (var conn = GetConn())
            {
                conn.Execute($"DELETE FROM {name}");
            }
        }

        protected override ITable<T> CreateITable<T>(string name)
        {
            return new SQLiteTable<T>(name, this);
        }
    }
}
