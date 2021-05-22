using System;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;
using Z.BulkOperations;
using Z.Dapper.Plus;

namespace Dapper.Sharding
{
    public class DapperEntity
    {
        public DapperEntity(string tableName, IDatabase database, IDbConnection conn = null, IDbTransaction tran = null, int? commandTimeout = null)
        {
            TableName = tableName;
            DataBase = database;
            Conn = conn;
            Tran = tran;
            CommandTimeout = commandTimeout;
        }

        private IDatabase DataBase { get; }

        private IDbConnection Conn { get; }

        private IDbTransaction Tran { get; }

        private int? CommandTimeout { get; }

        public string TableName { get; }

        #region no Async

        public int Execute(string sql, object param = null)
        {
            if (Conn == null)
            {
                using (var cnn = DataBase.GetConn())
                {
                    return cnn.Execute(sql, param);
                }
            }
            else
            {
                return Conn.Execute(sql, param, Tran, CommandTimeout);
            }

        }

        public object ExecuteScalar(string sql, object param = null)
        {
            if (Conn == null)
            {
                using (var cnn = DataBase.GetConn())
                {
                    return cnn.ExecuteScalar(sql, param);
                }
            }
            else
            {
                return Conn.ExecuteScalar(sql, param, Tran, CommandTimeout);
            }

        }

        public T ExecuteScalar<T>(string sql, object param = null)
        {
            if (Conn == null)
            {
                using (var cnn = DataBase.GetConn())
                {
                    return cnn.ExecuteScalar<T>(sql, param);
                }
            }
            else
            {
                return Conn.ExecuteScalar<T>(sql, param, Tran, CommandTimeout);
            }
        }

        public dynamic QueryFirstOrDefault(string sql, object param = null)
        {
            if (Conn == null)
            {
                using (var cnn = DataBase.GetConn())
                {
                    return cnn.QueryFirstOrDefault(sql, param);
                }
            }
            else
            {
                return Conn.QueryFirstOrDefault(sql, param, Tran, commandTimeout: CommandTimeout);
            }

        }

        public T QueryFirstOrDefault<T>(string sql, object param = null)
        {
            if (Conn == null)
            {
                using (var cnn = DataBase.GetConn())
                {
                    return cnn.QueryFirstOrDefault<T>(sql, param);
                }
            }
            else
            {
                return Conn.QueryFirstOrDefault<T>(sql, param, Tran, commandTimeout: CommandTimeout);
            }
        }

        public IEnumerable<dynamic> Query(string sql, object param = null)
        {
            if (Conn == null)
            {
                using (var cnn = DataBase.GetConn())
                {
                    return cnn.Query(sql, param);
                }
            }
            else
            {
                return Conn.Query(sql, param, Tran, commandTimeout: CommandTimeout);
            }

        }

        public IEnumerable<T> Query<T>(string sql, object param = null)
        {
            if (Conn == null)
            {
                using (var cnn = DataBase.GetConn())
                {
                    return cnn.Query<T>(sql, param);
                }
            }
            else
            {
                return Conn.Query<T>(sql, param, Tran, commandTimeout: CommandTimeout);
            }
        }

        public void QueryMultiple(string sql, object param = null, Action<SqlMapper.GridReader> onReader = null)
        {
            if (Conn == null)
            {
                using (var cnn = DataBase.GetConn())
                {
                    using (var reader = cnn.QueryMultiple(sql, param))
                    {
                        onReader?.Invoke(reader);
                    }
                }
            }
            else
            {
                using (var reader = Conn.QueryMultiple(sql, param, Tran, CommandTimeout))
                {
                    onReader?.Invoke(reader);
                }
            }
        }

        #endregion

        #region Async

        public Task<int> ExecuteAsync(string sql, object param = null)
        {
            if (Conn == null)
            {
                using (var cnn = DataBase.GetConn())
                {
                    return cnn.ExecuteAsync(sql, param);
                }
            }
            else
            {
                return Conn.ExecuteAsync(sql, param, Tran, CommandTimeout);
            }

        }

        public Task<object> ExecuteScalarAsync(string sql, object param = null)
        {
            if (Conn == null)
            {
                using (var cnn = DataBase.GetConn())
                {
                    return cnn.ExecuteScalarAsync(sql, param);
                }
            }
            else
            {
                return Conn.ExecuteScalarAsync(sql, param, Tran, CommandTimeout);
            }

        }

        public Task<T> ExecuteScalarAsync<T>(string sql, object param = null)
        {
            if (Conn == null)
            {
                using (var cnn = DataBase.GetConn())
                {
                    return cnn.ExecuteScalarAsync<T>(sql, param);
                }
            }
            else
            {
                return Conn.ExecuteScalarAsync<T>(sql, param, Tran, CommandTimeout);
            }
        }

        public Task<dynamic> QueryFirstOrDefaultAsync(string sql, object param = null)
        {
            if (Conn == null)
            {
                using (var cnn = DataBase.GetConn())
                {
                    return cnn.QueryFirstOrDefaultAsync(sql, param);
                }
            }
            else
            {
                return Conn.QueryFirstOrDefaultAsync(sql, param, Tran, commandTimeout: CommandTimeout);
            }

        }

        public Task<T> QueryFirstOrDefaultAsync<T>(string sql, object param = null)
        {
            if (Conn == null)
            {
                using (var cnn = DataBase.GetConn())
                {
                    return cnn.QueryFirstOrDefaultAsync<T>(sql, param);
                }
            }
            else
            {
                return Conn.QueryFirstOrDefaultAsync<T>(sql, param, Tran, commandTimeout: CommandTimeout);
            }
        }

        public Task<IEnumerable<dynamic>> QueryAsync(string sql, object param = null)
        {
            if (Conn == null)
            {
                using (var cnn = DataBase.GetConn())
                {
                    return cnn.QueryAsync(sql, param);
                }
            }
            else
            {
                return Conn.QueryAsync(sql, param, Tran, commandTimeout: CommandTimeout);
            }

        }

        public Task<IEnumerable<T>> QueryAsync<T>(string sql, object param = null)
        {
            if (Conn == null)
            {
                using (var cnn = DataBase.GetConn())
                {
                    return cnn.QueryAsync<T>(sql, param);
                }
            }
            else
            {
                return Conn.QueryAsync<T>(sql, param, Tran, commandTimeout: CommandTimeout);
            }
        }

        public async Task QueryMultipleAsync(string sql, object param = null, Action<SqlMapper.GridReader> onReader = null)
        {
            if (Conn == null)
            {
                using (var cnn = DataBase.GetConn())
                {
                    using (var reader = await cnn.QueryMultipleAsync(sql, param))
                    {
                        onReader?.Invoke(reader);
                    }
                }
            }
            else
            {
                using (var reader = await Conn.QueryMultipleAsync(sql, param, Tran, CommandTimeout))
                {
                    onReader?.Invoke(reader);
                }
            }
        }

        #endregion

        #region Bulk

        public void BulkInsert<T>(T model, Action<BulkOperation> action) where T : class
        {
            var key = DapperPlusUtils.Map<T>(TableName);
            if (Conn == null)
            {
                using (var cnn = DataBase.GetConn())
                {
                    cnn.UseBulkOptions(option =>
                    {
                        action(option);

                    }).BulkInsert(key, model);
                }
            }
            else
            {
                Tran.UseBulkOptions(option =>
                {
                    action(option);

                }).BulkInsert(key, model);
            }
        }

        public void BulkInsert<T>(IEnumerable<T> modelList, Action<BulkOperation> action) where T : class
        {
            var key = DapperPlusUtils.Map<T>(TableName);
            if (Conn == null)
            {
                using (var cnn = DataBase.GetConn())
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
                Tran.UseBulkOptions(option =>
                {
                    action(option);

                }).BulkInsert(key, modelList);
            }
        }

        public void BulkUpdate<T>(T model, Action<BulkOperation> action) where T : class
        {
            var key = DapperPlusUtils.Map<T>(TableName);
            if (Conn == null)
            {
                using (var cnn = DataBase.GetConn())
                {
                    cnn.UseBulkOptions(option =>
                    {
                        action(option);

                    }).BulkUpdate(key, model);

                }
            }
            else
            {
                Tran.UseBulkOptions(option =>
                {
                    action(option);

                }).BulkUpdate(key, model);
            }
        }

        public void BulkUpdate<T>(IEnumerable<T> modelList, Action<BulkOperation> action) where T : class
        {
            var key = DapperPlusUtils.Map<T>(TableName);
            if (Conn == null)
            {
                using (var cnn = DataBase.GetConn())
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
                Tran.UseBulkOptions(option =>
                {
                    action(option);

                }).BulkUpdate(key, modelList);
            }
        }

        public void BulkDelete<T>(T model, Action<BulkOperation> action) where T : class
        {
            var key = DapperPlusUtils.Map<T>(TableName);
            if (Conn == null)
            {
                using (var cnn = DataBase.GetConn())
                {
                    cnn.UseBulkOptions(option =>
                    {
                        action(option);

                    }).BulkDelete(key, model);
                }
            }
            else
            {
                Tran.UseBulkOptions(option =>
                {
                    action(option);

                }).BulkDelete(key, model);
            }
        }

        public void BulkDelete<T>(IEnumerable<T> modelList, Action<BulkOperation> action) where T : class
        {
            var key = DapperPlusUtils.Map<T>(TableName);
            if (Conn == null)
            {
                using (var cnn = DataBase.GetConn())
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
                Tran.UseBulkOptions(option =>
                {
                    action(option);

                }).BulkDelete(key, modelList);
            }
        }

        public void BulkMerge<T>(T model, Action<BulkOperation> action) where T : class
        {
            var key = DapperPlusUtils.Map<T>(TableName);
            if (Conn == null)
            {
                using (var cnn = DataBase.GetConn())
                {
                    cnn.UseBulkOptions(option =>
                    {
                        action(option);

                    }).BulkMerge(key, model);
                }
            }
            else
            {
                Tran.UseBulkOptions(option =>
                {
                    action(option);

                }).BulkMerge(key, model);
            }
        }

        public void BulkMerge<T>(IEnumerable<T> modelList, Action<BulkOperation> action) where T : class
        {
            var key = DapperPlusUtils.Map<T>(TableName);
            if (Conn == null)
            {
                using (var cnn = DataBase.GetConn())
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
                Tran.UseBulkOptions(option =>
                {
                    action(option);

                }).BulkMerge(key, modelList);
            }
        }

        #endregion



    }
}
