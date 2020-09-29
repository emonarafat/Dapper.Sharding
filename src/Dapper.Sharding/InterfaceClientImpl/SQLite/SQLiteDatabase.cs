using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SQLite;
using System.IO;
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
            throw new NotImplementedException();
        }

        public override string GetTableScript<T>(string name)
        {
            throw new NotImplementedException();
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
            throw new NotImplementedException();
        }
    }
}
