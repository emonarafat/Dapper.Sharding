using Npgsql;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

namespace Dapper.Sharding
{
    internal class PostgreClient : IClient
    {

        public PostgreClient(DataBaseConfig config) : base(DataBaseType.Postgresql, config)
        {
            ConnectionString = ConnectionStringBuilder.BuilderPostgresql(config);
        }

        public override string ConnectionString { get; }

        #region protected method

        protected override IDatabase CreateIDatabase(string name)
        {
            return new PostgreDatabase(name, this);
        }

        #endregion

        public override void CreateDatabase(string name)
        {
            using (var conn = GetConn())
            {
                var count = conn.ExecuteScalar<int>($"SELECT COUNT(1) FROM pg_database WHERE datname = '{name}'");
                if (count == 0)
                {
                    conn.Execute($"CREATE DATABASE {name}");
                }
            }
        }

        public override void DropDatabase(string name)
        {
            using (var conn = GetConn())
            {
                conn.Execute($"DROP DATABASE IF EXISTS {name}");
            }
        }

        public override bool ExistsDatabase(string name)
        {
            using (var conn = GetConn())
            {
                return conn.ExecuteScalar<int>($"SELECT COUNT(1) FROM pg_database WHERE datname = '{name}'") > 0;

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

        public override IEnumerable<string> ShowDatabases()
        {
            using (var conn = GetConn())
            {
                return conn.Query<string>("select pg_database.datname from pg_database");
            }
        }

        public override IEnumerable<string> ShowDatabasesExcludeSystem()
        {
            return ShowDatabases().Where(w => w != "template1" && w != "template0");
        }
    }
}
