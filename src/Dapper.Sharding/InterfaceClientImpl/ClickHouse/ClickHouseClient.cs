using ClickHouse.Ado;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

namespace Dapper.Sharding
{
    internal class ClickHouseClient : IClient
    {
        public ClickHouseClient(DataBaseConfig config) : base(DataBaseType.ClickHouse, config)
        {
            ConnectionString = ConnectionStringBuilder.BuilderClickHouse(config);
        }

        public override string ConnectionString { get; }

        public override string GetDatabaseScript(string name, bool useGis = false, string ext = null)
        {
            if (string.IsNullOrEmpty(ext))
            {
                return $"CREATE DATABASE IF NOT EXISTS {name}";
            }
            else
            {
                return $"CREATE DATABASE IF NOT EXISTS {name} {ext}";
            }
        }

        public override void CreateDatabase(string name, bool useGis = false, string ext = null)
        {
            var sql = GetDatabaseScript(name, useGis, ext);
            Execute(sql);
        }

        public override void DropDatabase(string name)
        {
            Execute($"DROP DATABASE IF EXISTS {name}");
            DataBaseCache.TryRemove(name, out _);
        }

        public override bool ExistsDatabase(string name)
        {
            var dbname = ExecuteScalar<string>($"SHOW DATABASES LIKE '{name}'");
            return !string.IsNullOrEmpty(dbname);
        }

        public override IDbConnection GetConn()
        {
            var conn = new ClickHouseConnection(ConnectionString);
            if (conn.State == ConnectionState.Closed)
            {
                try
                {
                    conn.Open();
                }
                catch (Exception ex)
                {
                    conn.Dispose();
                    throw ex;
                }
            }
            return conn;
        }

        public override Task<IDbConnection> GetConnAsync()
        {
            var conn = new ClickHouseConnection(ConnectionString);
            if (conn.State == ConnectionState.Closed)
            {
                try
                {
                    conn.Open();
                }
                catch (Exception ex)
                {
                    conn.Dispose();
                    throw ex;
                }
            }
            return Task.FromResult<IDbConnection>(conn);
        }

        public override IEnumerable<string> ShowDatabases()
        {
            return Query<string>("SHOW DATABASES");
        }

        public override IEnumerable<string> ShowDatabasesExcludeSystem()
        {
            return ShowDatabases().Where(w => w != "system");
        }

        public override void Vacuum(string dbname)
        {
            throw new NotImplementedException();
        }

        protected override IDatabase CreateIDatabase(string name)
        {
            return new ClickHouseDatabase(name, this);
        }
    }
}
