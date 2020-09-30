using MySqlConnector;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

namespace Dapper.Sharding
{
    internal class MySqlClient : IClient
    {

        public MySqlClient(DataBaseConfig config) : base(DataBaseType.MySql, config)
        {
            Charset = "utf8";
            ConnectionString = ConnectionStringBuilder.BuilderMySql(config);
        }

        #region protected method

        protected override IDatabase CreateIDatabase(string name)
        {
            return new MySqlDatabase(name, this);
        }

        #endregion

        public override string ConnectionString { get; }


        public override IDbConnection GetConn()
        {
            var conn = new MySqlConnection(ConnectionString);
            if (conn.State != ConnectionState.Open)
                conn.Open();
            return conn;
        }

        public override async Task<IDbConnection> GetConnAsync()
        {
            var conn = new MySqlConnection(ConnectionString);
            if (conn.State != ConnectionState.Open)
                await conn.OpenAsync();
            return conn;
        }

        public override void CreateDatabase(string name)
        {
            using (var conn = GetConn())
            {
                conn.Execute($"CREATE DATABASE IF NOT EXISTS `{name}` DEFAULT CHARACTER SET {Charset} COLLATE {Charset}_general_ci");
            }
        }

        public override void DropDatabase(string name)
        {
            using (var conn = GetConn())
            {
                conn.Execute($"DROP DATABASE IF EXISTS `{name}`");
            }
            DataBaseCache.TryRemove(name.ToLower(), out _);
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
            return ShowDatabases().Where(w => w != "mysql" && w != "information_schema" && w != "performance_schema" && w != "sys");
        }

        public override bool ExistsDatabase(string name)
        {
            using (var conn = GetConn())
            {
                return !string.IsNullOrEmpty(conn.QueryFirstOrDefault<string>($"SHOW DATABASES LIKE '{name}'"));
            }
        }

    }
}
