using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;

namespace Dapper.Sharding
{
    internal class SqlServerClient : IClient
    {

        public SqlServerClient(DataBaseConfig config, DataBaseType dbType) : base(dbType, config)
        {
            ConnectionString = ConnectionStringBuilder.BuilderSqlServer(config);
        }

        public override string ConnectionString { get; }


        #region protected method

        protected override IDatabase CreateIDatabase(string name)
        {
            return new SqlServerDatabase(name, this);
        }

        #endregion

        public override void CreateDatabase(string name)
        {
            using (var conn = GetConn())
            {
                conn.Execute($"IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name='{name}') CREATE DATABASE [{name}]");
            }
        }

        public override void DropDatabase(string name)
        {
            using (var conn = GetConn())
            {
                conn.Execute($"IF EXISTS (SELECT 1 FROM sys.databases WHERE name='{name}') DROP DATABASE [{name}]");
            }
            DataBaseCache.TryRemove(name.ToLower(), out _);
        }

        public override bool ExistsDatabase(string name)
        {
            using (var conn = GetConn())
            {
                return conn.ExecuteScalar($"SELECT 1 FROM sys.databases WHERE name='{name}'") != null;
            }
        }

        public override IDbConnection GetConn()
        {
            var conn = new SqlConnection(ConnectionString);
            if (conn.State != ConnectionState.Open)
                conn.Open();
            return conn;
        }

        public override async Task<IDbConnection> GetConnAsync()
        {
            var conn = new SqlConnection(ConnectionString);
            if (conn.State != ConnectionState.Open)
                await conn.OpenAsync();
            return conn;
        }

        public override IEnumerable<string> ShowDatabases()
        {
            using (var conn = GetConn())
            {
                return conn.Query<string>("SELECT name FROM sys.databases");
            }
        }

        public override IEnumerable<string> ShowDatabasesExcludeSystem()
        {
            return ShowDatabases().Where(w => w != "master" && w != "tempdb" && w != "model" && w != "msdb" && w.ToLower() != "resource");
        }


    }
}
