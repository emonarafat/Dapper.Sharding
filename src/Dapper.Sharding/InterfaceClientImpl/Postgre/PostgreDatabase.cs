using Npgsql;
using System;
using System.Collections.Generic;
using System.Data;
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
            using (var conn = GetConn())
            {
                conn.Execute($"DROP TABLE IF EXISTS {name}");
            }
            TableCache.TryRemove(name.ToLower(), out _);
        }

        public override bool ExistsTable(string name)
        {
            using (var conn = GetConn())
            {
                return conn.ExecuteScalar<int>($"select count(1) from pg_class where relname='{name}'") > 0;
            }
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
            using (var conn = GetConn())
            {
                return conn.Query<string>($"select column_name from information_schema.columns where table_schema='public' and table_name = '{name}'");
            }
        }

        public override TableEntity GetTableEntityFromDatabase(string name)
        {
            throw new NotImplementedException();
        }

        public override IEnumerable<string> GetTableList()
        {
            using (var conn = GetConn())
            {
                return conn.Query<string>("select tablename from pg_tables where schemaname='public'");
            }
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
            using (var conn = GetConn())
            {
                conn.Execute($"TRUNCATE TABLE {name}");
            }
        }


    }
}
