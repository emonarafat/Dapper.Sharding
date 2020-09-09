using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;

namespace Dapper.Sharding
{
    internal class SqlServerClient : IClient
    {
        public SqlServerClient(string connectionString, DataBaseType dbType)
        {
            ConnectionString = connectionString;
            Locker = new LockManager();
            DataBaseCache = new ConcurrentDictionary<string, IDatabase>();
            Charset = "utf8";
            AutoCreateDatabase = true;
            AutoCreateTable = true;
            AutoCompareTableColumn = false;
            DbType = dbType;
        }

        public LockManager Locker { get; }

        public ConcurrentDictionary<string, IDatabase> DataBaseCache { get; }

        public string ConnectionString { get; }

        public string Charset { get; set; }

        public bool AutoCreateDatabase { get; set; }

        public bool AutoCreateTable { get; set; }

        public bool AutoCompareTableColumn { get; set; }

        public DataBaseType DbType { get; }

        public void ClearCache()
        {
            foreach (var item in DataBaseCache.Keys)
            {
                GetDatabase(item).TableCache.Clear();
            }
            DataBaseCache.Clear();
        }

        public void CreateDatabase(string name)
        {
            using (var conn = GetConn())
            {
                conn.Execute($"IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name='{name}') CREATE DATABASE [{name}]");
            }
        }

        public void DropDatabase(string name)
        {
            using (var conn = GetConn())
            {
                conn.Execute($"IF EXISTS (SELECT 1 FROM sys.databases WHERE name='{name}') DROP DATABASE [{name}]");
            }
            DataBaseCache.TryRemove(name.ToLower(), out _);
        }

        public bool ExistsDatabase(string name)
        {
            using (var conn = GetConn())
            {
                return conn.ExecuteScalar($"SELECT 1 FROM sys.databases WHERE name='{name}'") != null;
            }
        }

        public IDbConnection GetConn()
        {
            var conn = new SqlConnection(ConnectionString);
            if (conn.State != ConnectionState.Open)
                conn.Open();
            return conn;
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
                        DataBaseCache.TryAdd(lowerName, new SqlServerDatabase(name, this));
                    }
                }
            }
            return DataBaseCache[lowerName];
        }

        public IEnumerable<string> ShowDatabases()
        {
            using (var conn = GetConn())
            {
                return conn.Query<string>("SELECT name FROM sys.databases");
            }
        }

        public IEnumerable<string> ShowDatabasesExcludeSystem()
        {
            return ShowDatabases().Where(w => w != "master" && w != "tempdb" && w != "model" && w != "msdb" && w.ToLower() != "resource");
        }
    }
}
