//using MySql.Data.MySqlClient;
using System.Collections.Generic;
using System.Data;

namespace Dapper.Sharding
{
    internal class MySqlClient : IClient
    {
        public LockManager Locker { get; } = new LockManager();

        public HashSet<string> DataBaseCache { get; } = new HashSet<string>();

        public HashSet<string> TableCache { get; } = new HashSet<string>();

        public MySqlClient(string connectionString)
        {
            ConnectionString = connectionString;
        }

        public IDbConnection GetConn()
        {
            //var conn = new MySqlConnection(ConnectionString);
            //if (conn.State != ConnectionState.Open)
            //    conn.Open();
            //return conn;
            return null;
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
            DataBaseCache.Remove(name);
        }

        public IEnumerable<string> GetAllDatabase()
        {
            using (var conn = GetConn())
            {
                return conn.Query<string>("SHOW DATABASES");
            }
        }

        public IDatabase GetDatabase(string name)
        {
            if (AutoCreateDatabase)
            {
                if (!DataBaseCache.Contains(name))
                {
                    lock (Locker.GetObject(name))
                    {
                        if (!DataBaseCache.Contains(name))
                        {
                            CreateDatabase(name);
                            DataBaseCache.Add(name);
                        }
                    }
                }
            }

            return new MySqlDatabase(name, this);
        }

        #endregion

    }
}
