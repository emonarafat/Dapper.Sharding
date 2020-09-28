using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;

namespace Dapper.Sharding
{
    public abstract class IClient
    {
        public IClient()
        {

        }

        public IClient(DataBaseType dbType, string connectionString)
        {
            DbType = dbType;
            ConnectionString = connectionString;
        }

        #region protected method

        private LockManager Locker { get; } = new LockManager();

        protected ConcurrentDictionary<string, IDatabase> DataBaseCache { get; } = new ConcurrentDictionary<string, IDatabase>();

        protected abstract IDatabase GetIDatabase(string name);

        #endregion

        #region common method

        public DataBaseType DbType { get; }

        public string ConnectionString { get; }

        public string Charset { get; set; }

        public bool AutoCreateDatabase { get; set; } = true;

        public bool AutoCreateTable { get; set; } = true;

        public bool AutoCompareTableColumn { get; set; } = false;

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
                        DataBaseCache.TryAdd(lowerName, GetIDatabase(name));
                    }
                }
            }
            return DataBaseCache[lowerName];
        }

        public void ClearCache()
        {
            DataBaseCache.Clear();
        }

        #endregion

        #region abstract method

        public abstract IDbConnection GetConn();

        public abstract Task<IDbConnection> GetConnAsync();

        public abstract void CreateDatabase(string name);

        public abstract void DropDatabase(string name);

        public abstract IEnumerable<string> ShowDatabases();

        public abstract IEnumerable<string> ShowDatabasesExcludeSystem();

        public abstract bool ExistsDatabase(string name);


        #endregion

    }
}
