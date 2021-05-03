﻿using System.Collections.Concurrent;
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

        public virtual IDatabase GetDatabase(string name, bool useGis = false, string gisExt = null)
        {
            var lowerName = name.ToLower();
            var exists = DataBaseCache.TryGetValue(lowerName, out var val);
            if (!exists)
            {
                lock (Locker.GetObject(lowerName))
                {
                    if (!DataBaseCache.ContainsKey(lowerName))
                    {
                        if (AutoCreateDatabase)
                        {
                            if (!ExistsDatabase(name))
                            {
                                CreateDatabase(name, useGis, gisExt);
                            }
                        }
                        val = CreateIDatabase(name);
                        DataBaseCache.TryAdd(lowerName, val);
                    }
                }
            }
            return val;
        }

        public void ClearCache()
        {
            DataBaseCache.Clear();
        }

        #endregion

        #region abstract method

        public abstract string ConnectionString { get; }

        public abstract IDbConnection GetConn();

        public abstract Task<IDbConnection> GetConnAsync();

        public abstract void CreateDatabase(string name, bool useGis = false, string gisExt = null);

        public abstract void DropDatabase(string name);

        public abstract IEnumerable<string> ShowDatabases();

        public abstract IEnumerable<string> ShowDatabasesExcludeSystem();

        public abstract bool ExistsDatabase(string name);


        #endregion

    }
}
