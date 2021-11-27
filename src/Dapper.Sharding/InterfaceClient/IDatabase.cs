using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using Z.BulkOperations;
using Z.Dapper.Plus;

namespace Dapper.Sharding
{
    public abstract class IDatabase
    {
        public IDatabase(string name, IClient client)
        {
            Name = name;
            Client = client;
        }

        #region dapper method

        public int Execute(string sql, object param = null, DistributedTransaction tran = null, int? timeout = null)
        {
            if (tran == null || DbType == DataBaseType.ClickHouse)
            {
                using (var cnn = GetConn())
                {
                    return cnn.Execute(sql, param, commandTimeout: timeout);
                }
            }
            var val = tran.GetVal(this);
            return val.Item1.Execute(sql, param, val.Item2, timeout);
        }

        public object ExecuteScalar(string sql, object param = null, DistributedTransaction tran = null, int? timeout = null)
        {
            if (tran == null || DbType == DataBaseType.ClickHouse)
            {
                using (var cnn = GetConn())
                {
                    return cnn.ExecuteScalar(sql, param, commandTimeout: timeout);
                }
            }
            var val = tran.GetVal(this);
            return val.Item1.ExecuteScalar(sql, param, val.Item2, timeout);
        }

        public T ExecuteScalar<T>(string sql, object param = null, DistributedTransaction tran = null, int? timeout = null)
        {
            if (tran == null || DbType == DataBaseType.ClickHouse)
            {
                using (var cnn = GetConn())
                {
                    return cnn.ExecuteScalar<T>(sql, param, commandTimeout: timeout);
                }
            }
            var val = tran.GetVal(this);
            return val.Item1.ExecuteScalar<T>(sql, param, val.Item2, timeout);
        }

        public dynamic QueryFirstOrDefault(string sql, object param = null, DistributedTransaction tran = null, int? timeout = null)
        {
            if (tran == null || DbType == DataBaseType.ClickHouse)
            {
                using (var cnn = GetConn())
                {
                    return cnn.QueryFirstOrDefault(sql, param, commandTimeout: timeout);
                }
            }
            var val = tran.GetVal(this);
            return val.Item1.QueryFirstOrDefault(sql, param, val.Item2, timeout);
        }

        public T QueryFirstOrDefault<T>(string sql, object param = null, DistributedTransaction tran = null, int? timeout = null)
        {
            if (tran == null || DbType == DataBaseType.ClickHouse)
            {
                using (var cnn = GetConn())
                {
                    return cnn.QueryFirstOrDefault<T>(sql, param, commandTimeout: timeout);
                }
            }
            var val = tran.GetVal(this);
            return val.Item1.QueryFirstOrDefault<T>(sql, param, val.Item2, timeout);
        }

        public IEnumerable<dynamic> Query(string sql, object param = null, DistributedTransaction tran = null, int? timeout = null)
        {
            if (tran == null || DbType == DataBaseType.ClickHouse)
            {
                using (var cnn = GetConn())
                {
                    return cnn.Query(sql, param, commandTimeout: timeout);
                }
            }
            var val = tran.GetVal(this);
            return val.Item1.Query(sql, param, val.Item2, commandTimeout: timeout);
        }

        public IEnumerable<T> Query<T>(string sql, object param = null, DistributedTransaction tran = null, int? timeout = null)
        {
            if (tran == null || DbType == DataBaseType.ClickHouse)
            {
                using (var cnn = GetConn())
                {
                    return cnn.Query<T>(sql, param, commandTimeout: timeout);
                }
            }
            var val = tran.GetVal(this);
            return val.Item1.Query<T>(sql, param, val.Item2, commandTimeout: timeout);
        }

        public void QueryMultiple(string sql, object param = null, Action<SqlMapper.GridReader> onReader = null, DistributedTransaction tran = null, int? timeout = null)
        {
            if (tran == null || DbType == DataBaseType.ClickHouse)
            {
                using (var conn = GetConn())
                {
                    using (var reader = conn.QueryMultiple(sql, param, commandTimeout: timeout))
                    {
                        onReader?.Invoke(reader);
                    }
                }
                return;
            }
            var val = tran.GetVal(this);
            using (var reader = val.Item1.QueryMultiple(sql, param, val.Item2, commandTimeout: timeout))
            {
                onReader?.Invoke(reader);
            }
        }

        public DataTable QueryDataTable(string sql, object param = null, DistributedTransaction tran = null, int? timeout = null)
        {
            if (tran == null || DbType == DataBaseType.ClickHouse)
            {
                using (var cnn = GetConn())
                {
                    return cnn.GetDataTable(sql, param, commandTimeout: timeout);
                }
            }
            var val = tran.GetVal(this);
            return val.Item1.GetDataTable(sql, param, val.Item2, timeout);
        }

        public DataSet QueryDataSet(string sql, object param = null, DistributedTransaction tran = null, int? timeout = null)
        {
            if (tran == null || DbType == DataBaseType.ClickHouse)
            {
                using (var cnn = GetConn())
                {
                    return cnn.GetDataSet(sql, param, commandTimeout: timeout);
                }
            }
            var val = tran.GetVal(this);
            return val.Item1.GetDataSet(sql, param, val.Item2, timeout);
        }

        #endregion

        #region dapper method async

        public async Task<int> ExecuteAsync(string sql, object param = null, DistributedTransaction tran = null, int? timeout = null)
        {
            if (DbType == DataBaseType.ClickHouse)
            {
                return await Task.Run(() =>
                {
                    return Execute(sql, param, null, timeout);
                });
            }
            if (tran == null)
            {
                using (var cnn = await GetConnAsync())
                {
                    return await cnn.ExecuteAsync(sql, param, commandTimeout: timeout);
                }
            }
            var val = await tran.GetValAsync(this);
            return await val.Item1.ExecuteAsync(sql, param, val.Item2, timeout);
        }

        public async Task<object> ExecuteScalarAsync(string sql, object param = null, DistributedTransaction tran = null, int? timeout = null)
        {
            if (DbType == DataBaseType.ClickHouse)
            {
                return await Task.Run(() =>
                {
                    return ExecuteScalar(sql, param, null, timeout);
                });
            }
            if (tran == null)
            {
                using (var cnn = await GetConnAsync())
                {
                    return await cnn.ExecuteScalarAsync(sql, param, commandTimeout: timeout);
                }
            }
            var val = await tran.GetValAsync(this);
            return await val.Item1.ExecuteScalarAsync(sql, param, val.Item2, timeout);
        }

        public async Task<T> ExecuteScalarAsync<T>(string sql, object param = null, DistributedTransaction tran = null, int? timeout = null)
        {
            if (DbType == DataBaseType.ClickHouse)
            {
                return await Task.Run(() =>
                {
                    return ExecuteScalar<T>(sql, param, null, timeout);
                });
            }
            if (tran == null)
            {
                using (var cnn = await GetConnAsync())
                {
                    return await cnn.ExecuteScalarAsync<T>(sql, param, commandTimeout: timeout);
                }
            }
            var val = await tran.GetValAsync(this);
            return await val.Item1.ExecuteScalarAsync<T>(sql, param, val.Item2, timeout);
        }

        public async Task<dynamic> QueryFirstOrDefaultAsync(string sql, object param = null, DistributedTransaction tran = null, int? timeout = null)
        {
            if (DbType == DataBaseType.ClickHouse)
            {
                return await Task.Run(() =>
                {
                    return QueryFirstOrDefault(sql, param, null, timeout);
                });
            }
            if (tran == null)
            {
                using (var cnn = await GetConnAsync())
                {
                    return await cnn.QueryFirstOrDefaultAsync(sql, param, commandTimeout: timeout);
                }
            }
            var val = await tran.GetValAsync(this);
            return await val.Item1.QueryFirstOrDefaultAsync(sql, param, val.Item2, timeout);
        }

        public async Task<T> QueryFirstOrDefaultAsync<T>(string sql, object param = null, DistributedTransaction tran = null, int? timeout = null)
        {
            if (DbType == DataBaseType.ClickHouse)
            {
                return await Task.Run(() =>
                {
                    return QueryFirstOrDefault<T>(sql, param, null, timeout);
                });
            }
            if (tran == null)
            {
                using (var cnn = await GetConnAsync())
                {
                    return await cnn.QueryFirstOrDefaultAsync<T>(sql, param, commandTimeout: timeout);
                }
            }
            var val = await tran.GetValAsync(this);
            return await val.Item1.QueryFirstOrDefaultAsync<T>(sql, param, val.Item2, timeout);
        }

        public async Task<IEnumerable<dynamic>> QueryAsync(string sql, object param = null, DistributedTransaction tran = null, int? timeout = null)
        {
            if (DbType == DataBaseType.ClickHouse)
            {
                return await Task.Run(() =>
                {
                    return Query(sql, param, null, timeout);
                });
            }
            if (tran == null)
            {
                using (var cnn = await GetConnAsync())
                {
                    return await cnn.QueryAsync(sql, param, commandTimeout: timeout);
                }
            }
            var val = await tran.GetValAsync(this);
            return await val.Item1.QueryAsync(sql, param, val.Item2, timeout);
        }

        public async Task<IEnumerable<T>> QueryAsync<T>(string sql, object param = null, DistributedTransaction tran = null, int? timeout = null)
        {
            if (DbType == DataBaseType.ClickHouse)
            {
                return await Task.Run(() =>
                {
                    return Query<T>(sql, param, null, timeout);
                });
            }
            if (tran == null)
            {
                using (var cnn = await GetConnAsync())
                {
                    return await cnn.QueryAsync<T>(sql, param, commandTimeout: timeout);
                }
            }
            var val = await tran.GetValAsync(this);
            return await val.Item1.QueryAsync<T>(sql, param, val.Item2, timeout);
        }

        public async Task QueryMultipleAsync(string sql, object param = null, Action<SqlMapper.GridReader> onReader = null, DistributedTransaction tran = null, int? timeout = null)
        {
            if (DbType == DataBaseType.ClickHouse)
            {
                await Task.Run(() =>
                {
                    QueryMultiple(sql, param, onReader, null, timeout);
                });
                return;
            }
            if (tran == null)
            {
                using (var conn = await GetConnAsync())
                {
                    using (var reader = await conn.QueryMultipleAsync(sql, param, commandTimeout: timeout))
                    {
                        onReader?.Invoke(reader);
                    }
                }
                return;
            }
            var val = await tran.GetValAsync(this);
            using (var reader = await val.Item1.QueryMultipleAsync(sql, param, val.Item2, timeout))
            {
                onReader?.Invoke(reader);
            }
        }

        public async Task<DataTable> QueryDataTableAsync(string sql, object param = null, DistributedTransaction tran = null, int? timeout = null)
        {
            if (DbType == DataBaseType.ClickHouse)
            {
                return await Task.Run(() =>
                {
                    return QueryDataTable(sql, param, null, timeout);
                });
            }
            if (tran == null)
            {
                using (var cnn = await GetConnAsync())
                {
                    return await cnn.GetDataTableAsync(sql, param, commandTimeout: timeout);
                }
            }
            var val = await tran.GetValAsync(this);
            return await val.Item1.GetDataTableAsync(sql, param, val.Item2, timeout);
        }

        public async Task<DataSet> QueryDataSetAsync(string sql, object param = null, DistributedTransaction tran = null, int? timeout = null)
        {
            if (DbType == DataBaseType.ClickHouse)
            {
                return await Task.Run(() =>
                {
                    return QueryDataSet(sql, param, null, timeout);
                });
            }
            if (tran == null)
            {
                using (var cnn = await GetConnAsync())
                {
                    return await cnn.GetDataSetAsync(sql, param, commandTimeout: timeout);
                }
            }
            var val = await tran.GetValAsync(this);
            return await val.Item1.GetDataSetAsync(sql, param, val.Item2, timeout);
        }


        #endregion

        #region z.dapper.plus method

        public void BulkInsert<T>(string tableName, T model, Action<BulkOperation> action, DistributedTransaction tran = null) where T : class
        {
            var key = DapperPlusUtils.Map<T>(tableName);
            if (tran == null)
            {
                using (var cnn = GetConn())
                {
                    cnn.UseBulkOptions(option =>
                    {
                        action(option);
                    }).BulkInsert(key, model);
                }
            }
            else
            {
                var val = tran.GetVal(this);
                val.Item2.UseBulkOptions(option =>
                {
                    action(option);

                }).BulkInsert(key, model);
            }
        }

        public void BulkInsert<T>(string tableName, IEnumerable<T> modelList, Action<BulkOperation> action, DistributedTransaction tran = null) where T : class
        {
            var key = DapperPlusUtils.Map<T>(tableName);
            if (tran == null)
            {
                using (var cnn = GetConn())
                {
                    using (var tr = cnn.BeginTransaction())
                    {
                        try
                        {
                            tr.UseBulkOptions(option =>
                            {
                                action(option);

                            }).BulkInsert(key, modelList);
                            tr.Commit();
                        }
                        catch (Exception ex)
                        {
                            tr.Rollback();
                            throw ex;
                        }
                    }

                }
            }
            else
            {
                var val = tran.GetVal(this);
                val.Item2.UseBulkOptions(option =>
                {
                    action(option);

                }).BulkInsert(key, modelList);
            }
        }

        public void BulkUpdate<T>(string tableName, T model, Action<BulkOperation> action, DistributedTransaction tran = null) where T : class
        {
            var key = DapperPlusUtils.Map<T>(tableName);
            if (tran == null)
            {
                using (var cnn = GetConn())
                {
                    cnn.UseBulkOptions(option =>
                    {
                        action(option);

                    }).BulkUpdate(key, model);
                }
            }
            else
            {
                var val = tran.GetVal(this);
                val.Item2.UseBulkOptions(option =>
                {
                    action(option);

                }).BulkUpdate(key, model);
            }
        }

        public void BulkUpdate<T>(string tableName, IEnumerable<T> modelList, Action<BulkOperation> action, DistributedTransaction tran = null) where T : class
        {
            var key = DapperPlusUtils.Map<T>(tableName);
            if (tran == null)
            {
                using (var cnn = GetConn())
                {
                    using (var tr = cnn.BeginTransaction())
                    {
                        try
                        {
                            tr.UseBulkOptions(option =>
                            {
                                action(option);

                            }).BulkUpdate(key, modelList);
                            tr.Commit();
                        }
                        catch (Exception ex)
                        {
                            tr.Rollback();
                            throw ex;
                        }
                    }

                }
            }
            else
            {
                var val = tran.GetVal(this);
                val.Item2.UseBulkOptions(option =>
                {
                    action(option);

                }).BulkUpdate(key, modelList);
            }
        }

        public void BulkDelete<T>(string tableName, T model, Action<BulkOperation> action, DistributedTransaction tran = null) where T : class
        {
            var key = DapperPlusUtils.Map<T>(tableName);
            if (tran == null)
            {
                using (var cnn = GetConn())
                {
                    cnn.UseBulkOptions(option =>
                    {
                        action(option);

                    }).BulkDelete(key, model);
                }
            }
            else
            {
                var val = tran.GetVal(this);
                val.Item2.UseBulkOptions(option =>
                {
                    action(option);

                }).BulkDelete(key, model);
            }
        }

        public void BulkDelete<T>(string tableName, IEnumerable<T> modelList, Action<BulkOperation> action, DistributedTransaction tran = null) where T : class
        {
            var key = DapperPlusUtils.Map<T>(tableName);
            if (tran == null)
            {
                using (var cnn = GetConn())
                {
                    using (var tr = cnn.BeginTransaction())
                    {
                        try
                        {
                            tr.UseBulkOptions(option =>
                            {
                                action(option);

                            }).BulkDelete(key, modelList);
                            tr.Commit();
                        }
                        catch (Exception ex)
                        {
                            tr.Rollback();
                            throw ex;
                        }
                    }
                }
            }
            else
            {
                var val = tran.GetVal(this);
                val.Item2.UseBulkOptions(option =>
                {
                    action(option);

                }).BulkDelete(key, modelList);
            }
        }

        public void BulkMerge<T>(string tableName, T model, Action<BulkOperation> action, DistributedTransaction tran = null) where T : class
        {
            var key = DapperPlusUtils.Map<T>(tableName);
            if (tran == null)
            {
                using (var cnn = GetConn())
                {
                    cnn.UseBulkOptions(option =>
                    {
                        action(option);

                    }).BulkMerge(key, model);
                }
            }
            else
            {
                var val = tran.GetVal(this);
                val.Item2.UseBulkOptions(option =>
                {
                    action(option);

                }).BulkMerge(key, model);
            }
        }

        public void BulkMerge<T>(string tableName, IEnumerable<T> modelList, Action<BulkOperation> action, DistributedTransaction tran = null) where T : class
        {
            var key = DapperPlusUtils.Map<T>(tableName);
            if (tran == null)
            {
                using (var cnn = GetConn())
                {
                    using (var tr = cnn.BeginTransaction())
                    {
                        try
                        {
                            tr.UseBulkOptions(option =>
                            {
                                action(option);

                            }).BulkMerge(key, modelList);
                            tr.Commit();
                        }
                        catch (Exception ex)
                        {
                            tr.Rollback();
                            throw ex;
                        }
                    }
                }
            }
            else
            {
                var val = tran.GetVal(this);
                val.Item2.UseBulkOptions(option =>
                {
                    action(option);

                }).BulkMerge(key, modelList);
            }
        }

        #endregion

        #region z.dapper.plus method async

        public Task BulkInsertAsync<T>(string tableName, T model, Action<BulkOperation> action, DistributedTransaction tran = null) where T : class
        {
            return Task.Run(() =>
            {
                BulkInsert(tableName, model, action, tran);
            });
        }

        public Task BulkInsertAsync<T>(string tableName, IEnumerable<T> modelList, Action<BulkOperation> action, DistributedTransaction tran = null) where T : class
        {
            return Task.Run(() =>
            {
                BulkInsert(tableName, modelList, action, tran);
            });
        }

        public Task BulkUpdateAsync<T>(string tableName, T model, Action<BulkOperation> action, DistributedTransaction tran = null) where T : class
        {
            return Task.Run(() =>
            {
                BulkUpdate(tableName, model, action, tran);
            });
        }

        public Task BulkUpdateAsync<T>(string tableName, IEnumerable<T> modelList, Action<BulkOperation> action, DistributedTransaction tran = null) where T : class
        {
            return Task.Run(() =>
            {
                BulkUpdate(tableName, modelList, action, tran);
            });
        }

        public Task BulkDeleteAsync<T>(string tableName, T model, Action<BulkOperation> action, DistributedTransaction tran = null) where T : class
        {
            return Task.Run(() =>
            {
                BulkDelete(tableName, model, action, tran);
            });
        }

        public Task BulkDeleteAsync<T>(string tableName, IEnumerable<T> modelList, Action<BulkOperation> action, DistributedTransaction tran = null) where T : class
        {
            return Task.Run(() =>
            {
                BulkDelete(tableName, modelList, action, tran);
            });
        }

        public Task BulkMergeAsync<T>(string tableName, T model, Action<BulkOperation> action, DistributedTransaction tran = null) where T : class
        {
            return Task.Run(() =>
            {
                BulkMerge(tableName, model, action, tran);
            });
        }

        public Task BulkMergeAsync<T>(string tableName, IEnumerable<T> modelList, Action<BulkOperation> action, DistributedTransaction tran = null) where T : class
        {
            return Task.Run(() =>
            {
                BulkMerge(tableName, modelList, action, tran);
            });
        }

        #endregion

        #region protected method

        protected readonly LockManager Locker = new LockManager();

        protected readonly ConcurrentDictionary<string, object> TableCache = new ConcurrentDictionary<string, object>();

        protected readonly ConcurrentDictionary<string, object> TableCache2 = new ConcurrentDictionary<string, object>();

        protected abstract ITable<T> CreateITable<T>(string name) where T : class;

        #endregion

        #region public method

        public string Name { get; }

        public IClient Client { get; }

        public DataBaseType DbType
        {
            get
            {
                return Client.DbType;
            }
        }

        public void Using(Action<IDbConnection> action)
        {
            using (var conn = GetConn())
            {
                action(conn);
            }
        }

        public TResult Using<TResult>(Func<IDbConnection, TResult> func)
        {
            using (var conn = GetConn())
            {
                return func(conn);
            }
        }

        public void UsingTran(Action<IDbConnection, IDbTransaction> action)
        {
            using (var conn = GetConn())
            {
                using (var tran = conn.BeginTransaction())
                {
                    action(conn, tran);
                }
            }
        }

        public TResult UsingTran<TResult>(Func<IDbConnection, IDbTransaction, TResult> func)
        {
            using (var conn = GetConn())
            {
                using (var tran = conn.BeginTransaction())
                {
                    return func(conn, tran);
                }
            }
        }

        public virtual void CreateTable<T>(string name)
        {
            var script = GetTableScript<T>(name);
            Execute(script);
        }

        public ITable<T> GetTable<T>(string name) where T : class
        {
            var exists = TableCache.TryGetValue(name, out var val);
            if (!exists)
            {
                lock (Locker.GetObject(name))
                {
                    if (!TableCache.ContainsKey(name))
                    {
                        if (Client.AutoCreateTable)
                        {
                            #region 创建表、对比表

                            if (!ExistsTable(name))
                            {
                                CreateTable<T>(name);
                            }
                            else if (Client.AutoCompareTableColumn)
                            {
                                var dbColumns = GetTableColumnList(name);
                                var tableEntity = ClassToTableEntityUtils.Get<T>(Client.DbType);
                                var manager = GetTableManager(name);

                                foreach (var item in tableEntity.ColumnList)
                                {
                                    if (!dbColumns.Any(a => a.ToLower().Equals(item.Name.ToLower())))
                                    {
                                        manager.AddColumn(item.Name, item.CsType, item.Length, item.Comment, item.DbType);
                                    }
                                }

                                if (Client.AutoCompareTableColumnDelete)
                                {
                                    foreach (var item in dbColumns)
                                    {
                                        if (!tableEntity.ColumnList.Any(a => a.Name.ToLower().Equals(item.ToLower())))
                                        {
                                            manager.DropColumn(item);
                                        }
                                    }
                                }
                            }

                            #endregion
                        }
                        TableCache.TryAdd(name, CreateITable<T>(name));
                    }
                }
                val = TableCache[name];
            }
            return (ITable<T>)val;
        }

        public ITable<T> GetTableExist<T>(string name) where T : class
        {
            var key = typeof(T).FullName + "@" + name;
            var exists = TableCache2.TryGetValue(key, out var val);
            if (!exists)
            {
                lock (Locker.GetObject(key))
                {
                    if (!TableCache2.ContainsKey(key))
                    {
                        TableCache2.TryAdd(key, CreateITable<T>(name));
                    }
                }
                val = TableCache2[key];
            }
            return (ITable<T>)val;
        }

        public void GeneratorTableFile(string savePath, List<string> tableList = null, string nameSpace = "Model", string suffix = "", bool partialClass = false, bool fisrtCharUpper = false)
        {
            this.CreateTableFiles(savePath, tableList, nameSpace, suffix, partialClass, fisrtCharUpper);
        }

        public void GeneratorDbContextFile(string savePath, string nameSpace = "Model", string modelNameSpace = "Model", string modelSuffix = "", bool proSuffix = false, bool tableNameLower = false, bool staticClass = true)
        {
            this.CreateDbContextFile(savePath, nameSpace, modelNameSpace, modelSuffix, proSuffix, tableNameLower, staticClass);
        }

        public void ClearCache(string tablename = null)
        {
            if (!string.IsNullOrEmpty(tablename))
            {
                TableCache.TryRemove(tablename, out _);
            }
            else
            {
                TableCache.Clear();
            }

        }

        #endregion

        #region abstract method

        public abstract string ConnectionString { get; }

        public abstract IDbConnection GetConn();

        public abstract Task<IDbConnection> GetConnAsync();

        public abstract void DropTable(string name);

        public abstract void TruncateTable(string name);

        public abstract IEnumerable<string> GetTableList();

        public abstract IEnumerable<string> GetTableColumnList(string name);

        public abstract bool ExistsTable(string name);

        public abstract string GetTableScript<T>(string name);

        public abstract TableEntity GetTableEntityFromDatabase(string name, bool firstCharToUpper = false);

        public abstract ITableManager GetTableManager(string name);

        public abstract void OptimizeTable(string name, bool final = false, bool deduplicate = false);

        public abstract void OptimizeTable(string name, string partition, bool final = false, bool deduplicate = false);

        public abstract void Vacuum();

        #endregion

    }
}
