using Npgsql;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dapper.Sharding
{
    internal class PostgreDatabase : IDatabase
    {
        public PostgreDatabase(string name, PostgreClient client) : base(name, client)
        {
            ConnectionString = ConnectionStringBuilder.BuilderPostgresql(client.Config, name);
        }

        public override string ConnectionString { get; }

        protected override ITable<T> CreateITable<T>(string name)
        {
            return new PostgreTable<T>(name, this);
        }

        public override ITableManager GetTableManager(string name)
        {
            return new PostgreTableManager(name, this);
        }

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
            var conn = new NpgsqlConnection(ConnectionString);
            if (conn.State != ConnectionState.Open)
                conn.Open();
            return conn;
        }

        public override async Task<IDbConnection> GetConnAsync()
        {
            var conn = new NpgsqlConnection(ConnectionString);
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


    }
}
