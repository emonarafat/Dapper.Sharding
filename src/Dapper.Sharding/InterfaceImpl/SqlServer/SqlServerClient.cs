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
            DbType = dbType;
        }

        public LockManager Locker { get; } = new LockManager();

        public ConcurrentDictionary<string, IDatabase> DataBaseCache { get; } = new ConcurrentDictionary<string, IDatabase>();

        public string ConnectionString { get; }

        public string Charset { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

        public bool AutoCreateDatabase { get; set; } = true;

        public bool AutoCreateTable { get; set; } = true;

        public bool AutoCompareTableColumn { get; set; } = false;

        public DataBaseType DbType { get; }

        public void ClearCache()
        {
            DataBaseCache.Clear();
            var databaseList = ShowDatabasesExcludeSystem();
            foreach (var item in databaseList)
            {
                GetDatabase(item).TableCache.Clear();
            }
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
