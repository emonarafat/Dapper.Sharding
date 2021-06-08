using System;
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

        #region dapper method

        public int Execute(string sql, object param = null, int? commandTimeout = null)
        {
            using (var cnn = GetConn())
            {
                return cnn.Execute(sql, param, commandTimeout: commandTimeout);
            }
        }

        public object ExecuteScalar(string sql, object param = null, int? commandTimeout = null)
        {
            using (var cnn = GetConn())
            {
                return cnn.ExecuteScalar(sql, param, commandTimeout: commandTimeout);
            }

        }

        public T ExecuteScalar<T>(string sql, object param = null, int? commandTimeout = null)
        {
            using (var cnn = GetConn())
            {
                return cnn.ExecuteScalar<T>(sql, param, commandTimeout: commandTimeout);
            }
        }

        public dynamic QueryFirstOrDefault(string sql, object param = null, int? commandTimeout = null)
        {
            using (var cnn = GetConn())
            {
                return cnn.QueryFirstOrDefault(sql, param, commandTimeout: commandTimeout);
            }

        }

        public T QueryFirstOrDefault<T>(string sql, object param = null, int? commandTimeout = null)
        {
            using (var cnn = GetConn())
            {
                return cnn.QueryFirstOrDefault<T>(sql, param, commandTimeout: commandTimeout);
            }
        }

        public IEnumerable<dynamic> Query(string sql, object param = null, int? commandTimeout = null)
        {
            using (var cnn = GetConn())
            {
                return cnn.Query(sql, param, commandTimeout: commandTimeout);
            }

        }

        public IEnumerable<T> Query<T>(string sql, object param = null, int? commandTimeout = null)
        {
            using (var cnn = GetConn())
            {
                return cnn.Query<T>(sql, param, commandTimeout: commandTimeout);
            }
        }

        public void QueryMultiple(string sql, object param = null, Action<SqlMapper.GridReader> onReader = null, int? commandTimeout = null)
        {
            using (var conn = GetConn())
            {
                using (var reader = conn.QueryMultiple(sql, param, commandTimeout: commandTimeout))
                {
                    onReader?.Invoke(reader);
                }
            }
        }

        #endregion

        #region dapper method async

        public async Task<int> ExecuteAsync(string sql, object param = null, int? commandTimeout = null)
        {
            if (DbType == DataBaseType.ClickHouse)
            {
                return await Task.Run(() =>
                {
                    return Execute(sql, param, commandTimeout);
                });

            }
            using (var cnn = await GetConnAsync())
            {
                return await cnn.ExecuteAsync(sql, param, commandTimeout: commandTimeout);
            }

        }

        public async Task<object> ExecuteScalarAsync(string sql, object param = null, int? commandTimeout = null)
        {
            if (DbType == DataBaseType.ClickHouse)
            {
                return await Task.Run(() =>
                {
                    return ExecuteScalar(sql, param, commandTimeout);
                });
            }
            using (var cnn = await GetConnAsync())
            {
                return await cnn.ExecuteScalarAsync(sql, param, commandTimeout: commandTimeout);
            }

        }

        public async Task<T> ExecuteScalarAsync<T>(string sql, object param = null, int? commandTimeout = null)
        {
            if (DbType == DataBaseType.ClickHouse)
            {
                return await Task.Run(() =>
                {
                    return ExecuteScalar<T>(sql, param, commandTimeout);
                });
            }
            using (var cnn = await GetConnAsync())
            {
                return await cnn.ExecuteScalarAsync<T>(sql, param, commandTimeout: commandTimeout);
            }
        }

        public async Task<dynamic> QueryFirstOrDefaultAsync(string sql, object param = null, int? commandTimeout = null)
        {
            if (DbType == DataBaseType.ClickHouse)
            {
                return await Task.Run(() =>
                {
                    return QueryFirstOrDefault(sql, param, commandTimeout);
                });
            }
            using (var cnn = await GetConnAsync())
            {
                return await cnn.QueryFirstOrDefaultAsync(sql, param, commandTimeout: commandTimeout);
            }

        }

        public async Task<T> QueryFirstOrDefaultAsync<T>(string sql, object param = null, int? commandTimeout = null)
        {
            if (DbType == DataBaseType.ClickHouse)
            {
                return await Task.Run(() =>
                {
                    return QueryFirstOrDefault<T>(sql, param, commandTimeout);
                });
            }
            using (var cnn = await GetConnAsync())
            {
                return await cnn.QueryFirstOrDefaultAsync<T>(sql, param, commandTimeout: commandTimeout);
            }
        }

        public async Task<IEnumerable<dynamic>> QueryAsync(string sql, object param = null, int? commandTimeout = null)
        {
            if (DbType == DataBaseType.ClickHouse)
            {
                return await Task.Run(() =>
                {
                    return Query(sql, param, commandTimeout);
                });
            }
            using (var cnn = await GetConnAsync())
            {
                return await cnn.QueryAsync(sql, param, commandTimeout: commandTimeout);
            }

        }

        public async Task<IEnumerable<T>> QueryAsync<T>(string sql, object param = null, int? commandTimeout = null)
        {
            if (DbType == DataBaseType.ClickHouse)
            {
                return await Task.Run(() =>
                {
                    return Query<T>(sql, param, commandTimeout);
                });
            }
            using (var cnn = await GetConnAsync())
            {
                return await cnn.QueryAsync<T>(sql, param, commandTimeout: commandTimeout);
            }
        }

        public async Task QueryMultipleAsync(string sql, object param = null, Action<SqlMapper.GridReader> onReader = null, int? commandTimeout = null)
        {
            if (DbType == DataBaseType.ClickHouse)
            {
                await Task.Run(() =>
                {
                    QueryMultiple(sql, param, onReader, commandTimeout);
                });
                return;
            }
            using (var conn = await GetConnAsync())
            {
                using (var reader = await conn.QueryMultipleAsync(sql, param, commandTimeout: commandTimeout))
                {
                    onReader?.Invoke(reader);
                }
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
