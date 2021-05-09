using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using ClickHouse.Ado;

namespace Dapper.Sharding
{
    internal class ClickHouseClient : IClient
    {
        public ClickHouseClient(DataBaseConfig config) : base(DataBaseType.ClickHouse, config)
        {
            ConnectionString = ConnectionStringBuilder.BuilderClickHouse(config);
        }

        public override string ConnectionString { get; }

        public override void CreateDatabase(string name, bool useGis = false, string gisExt = null)
        {
            using (var conn = GetConn())
            {
                conn.Execute($"CREATE DATABASE IF NOT EXISTS {name}");
            }
        }

        public override void DropDatabase(string name)
        {
            using (var conn = GetConn())
            {
                conn.Execute($"DROP DATABASE IF EXISTS {name}");
            }
            DataBaseCache.TryRemove(name, out _);
        }

        public override bool ExistsDatabase(string name)
        {
            using (var conn = GetConn())
            {
                var dbname = conn.ExecuteScalar<string>($"SHOW DATABASES LIKE '{name}'");
                return !string.IsNullOrEmpty(dbname);
            }
        }

        public override IDbConnection GetConn()
        {
            var conn = new ClickHouseConnection(ConnectionString);
            if (conn.State != ConnectionState.Open)
                conn.Open();
            return conn;
        }

        public override Task<IDbConnection> GetConnAsync()
        {
            throw new NotImplementedException();
        }

        public override IEnumerable<string> ShowDatabases()
        {
            using (var conn = GetConn())
            {
                return conn.Query<string>("SHOW DATABASES");
            }
        }

        public override IEnumerable<string> ShowDatabasesExcludeSystem()
        {
            return ShowDatabases().Where(w => w != "system");
        }

        protected override IDatabase CreateIDatabase(string name)
        {
            return new ClickHouseDatabase(name, this);
        }
    }
}
