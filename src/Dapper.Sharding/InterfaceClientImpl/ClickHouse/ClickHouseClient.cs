using System;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;
using ClickHouse.Ado;

namespace Dapper.Sharding
{
    public class ClickHouseClient : IClient
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
            throw new NotImplementedException();
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
            throw new NotImplementedException();
        }

        public override IEnumerable<string> ShowDatabasesExcludeSystem()
        {
            throw new NotImplementedException();
        }

        protected override IDatabase CreateIDatabase(string name)
        {
            throw new NotImplementedException();
        }
    }
}
