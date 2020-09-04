using MySqlConnector;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace Dapper.Sharding
{
    internal class MySqlClient : IClient
    {
        public LockManager Locker { get; } = new LockManager();

        public ConcurrentDictionary<string, IDatabase> DataBaseCache { get; } = new ConcurrentDictionary<string, IDatabase>();

        public MySqlClient(string connectionString)
        {
            ConnectionString = connectionString;
        }

        public IDbConnection GetConn()
        {
            var conn = new MySqlConnection(ConnectionString);
            if (conn.State != ConnectionState.Open)
                conn.Open();
            return conn;
        }

        #region Interface Method

        public bool AutoCreateDatabase { get; set; } = true;

        public bool AutoCreateTable { get; set; } = true;

        public string Charset { get; set; } = "utf8";

        public bool AutoCompareTableColumn { get; set; } = false;

        public DataBaseType DbType => DataBaseType.MySql;

        public string ConnectionString { get; }


        public void CreateDatabase(string name)
        {
            using (var conn = GetConn())
            {
                conn.Execute($"CREATE DATABASE IF NOT EXISTS `{name}` DEFAULT CHARACTER SET {Charset} COLLATE {Charset}_general_ci");
            }
        }

        public void DropDatabase(string name)
        {
            using (var conn = GetConn())
            {
                conn.Execute($"DROP DATABASE IF EXISTS `{name}`");
            }

            DataBaseCache.TryRemove(name, out _);
        }

        public IEnumerable<string> ShowDatabases()
        {
            using (var conn = GetConn())
            {
                return conn.Query<string>("SHOW DATABASES");
            }
        }

        public IEnumerable<string> ShowDatabasesExcludeSystem()
        {
            return ShowDatabases().Where(w => w != "mysql" && w != "information_schema" && w != "performance_schema" && w != "sys");
        }

        public bool ExistsDatabase(string name)
        {
            using (var conn = GetConn())
            {
                return !string.IsNullOrEmpty(conn.QueryFirstOrDefault<string>($"SHOW DATABASES LIKE '{name}'"));
            }
        }

        public IDatabase GetDatabase(string name)
        {
            var lowerName = name.ToLower();
            if (!DataBaseCache.ContainsKey(lowerName))
            {
                lock (Locker.GetObject(lowerName))
                {
                    if (!DataBaseCache.ContainsKey(lowerName))
                    {
                        if (AutoCreateDatabase)
                        {
                            CreateDatabase(name);
                        }
                        DataBaseCache.TryAdd(lowerName, new MySqlDatabase(name, this));
                    }
                }
            }
            return DataBaseCache[lowerName];
        }

        public void ClearCache()
        {
            DataBaseCache.Clear();
            var databaseList = ShowDatabasesExcludeSystem();
            foreach (var item in databaseList)
            {
                GetDatabase(item).TableCache.Clear();
            }
        }


        #endregion

    }
}
