using MySqlConnector;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace Dapper.Sharding
{
    internal class MySqlClient : IClient
    {

        public MySqlClient(string connectionString)
        {
            ConnectionString = connectionString;
            Locker = new LockManager();
            DataBaseCache = new ConcurrentDictionary<string, IDatabase>();
            Charset = "utf8";
            AutoCreateDatabase = true;
            AutoCreateTable = true;
            AutoCompareTableColumn = false;
            DbType = DataBaseType.MySql;
        }

        #region Interface Method

        public LockManager Locker { get; }

        public ConcurrentDictionary<string, IDatabase> DataBaseCache { get; }

        public bool AutoCreateDatabase { get; set; }

        public bool AutoCreateTable { get; set; }

        public string Charset { get; set; }

        public bool AutoCompareTableColumn { get; set; }

        public DataBaseType DbType { get; }

        public string ConnectionString { get; }

        public IDbConnection GetConn()
        {
            var conn = new MySqlConnection(ConnectionString);
            if (conn.State != ConnectionState.Open)
                conn.Open();
            return conn;
        }

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
            DataBaseCache.TryRemove(name.ToLower(), out _);
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
                GetDatabase(item.ToLower()).TableCache.Clear();
            }
        }


        #endregion

    }
}
