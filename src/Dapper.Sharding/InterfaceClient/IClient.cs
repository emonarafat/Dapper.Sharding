using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;

namespace Dapper.Sharding
{
    public abstract class IClient
    {

        public IClient(DataBaseType dbType, DataBaseConfig config)
        {
            DbType = dbType;
            Config = config;
        }

        #region protected method

        protected LockManager Locker { get; } = new LockManager();

        protected ConcurrentDictionary<string, IDatabase> DataBaseCache { get; } = new ConcurrentDictionary<string, IDatabase>();

        protected abstract IDatabase CreateIDatabase(string name);


        #endregion

        #region common method

        public DataBaseType DbType { get; }

        public DataBaseConfig Config { get; }

        public string Charset { get; set; }

        public bool AutoCreateDatabase { get; set; } = true;

        public bool AutoCreateTable { get; set; } = true;

        public bool AutoCompareTableColumn { get; set; } = false;

        public virtual IDatabase GetDatabase(string name, bool useGis = false, string ext = null)
        {
            var exists = DataBaseCache.TryGetValue(name, out var val);
            if (!exists)
            {
                lock (Locker.GetObject(name))
                {
                    if (!DataBaseCache.ContainsKey(name))
                    {
                        if (AutoCreateDatabase)
                        {
                            if (!ExistsDatabase(name))
                            {
                                CreateDatabase(name, useGis, ext);
                            }
                        }
                        val = CreateIDatabase(name);
                        DataBaseCache.TryAdd(name, val);
                    }
                }
            }
            return val;
        }

        public void ClearCache(string dbname = null)
        {
            if (!string.IsNullOrEmpty(dbname))
            {
                DataBaseCache.TryRemove(dbname, out _);    
            }
            else
            {
                DataBaseCache.Clear();
            }
        }

        #endregion

        #region abstract method

        public abstract string ConnectionString { get; }

        public abstract IDbConnection GetConn();

        public abstract Task<IDbConnection> GetConnAsync();

        public abstract void CreateDatabase(string name, bool useGis = false, string ext = null);

        public abstract void DropDatabase(string name);

        public abstract IEnumerable<string> ShowDatabases();

        public abstract IEnumerable<string> ShowDatabasesExcludeSystem();

        public abstract bool ExistsDatabase(string name);


        #endregion

    }
}
