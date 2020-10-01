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
            throw new NotImplementedException();
        }

        public override bool ExistsTable(string name)
        {
            throw new NotImplementedException();
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
            throw new NotImplementedException();
        }

        public override TableEntity GetTableEntityFromDatabase(string name)
        {
            throw new NotImplementedException();
        }

        public override IEnumerable<string> GetTableList()
        {
            throw new NotImplementedException();
        }

        public override ITableManager GetTableManager(string name)
        {
            return new SQLiteTableManager(name, this);
        }

        public override string GetTableScript<T>(string name)
        {
            var tableEntity = ClassToTableEntityUtils.Get<T>();
            var sb = new StringBuilder();
            sb.Append($"CREATE TABLE IF NOT EXISTS [{name}] (");
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

        public override void SetCharset(string chartset)
        {
            throw new NotImplementedException();
        }

        public override void TruncateTable(string name)
        {
            
        }

        protected override ITable<T> CreateITable<T>(string name)
        {
            return new SQLiteTable<T>(name, this);
        }
    }
}
