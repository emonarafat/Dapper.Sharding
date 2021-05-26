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

        #region dapper no Async

        public int Execute(string sql, object param = null, int? commandTimeout = null)
        {
            if (Conn == null)
            {
                return DataBase.Execute(sql, param, commandTimeout);
            }
            return Conn.Execute(sql, param, Tran, CommandTimeout);

        }

        public object ExecuteScalar(string sql, object param = null, int? commandTimeout = null)
        {
            if (Conn == null)
            {
                return DataBase.ExecuteScalar(sql, param, commandTimeout);
            }
            return Conn.ExecuteScalar(sql, param, Tran, CommandTimeout);

        }

        public T ExecuteScalar<T>(string sql, object param = null, int? commandTimeout = null)
        {
            if (Conn == null)
            {
                return DataBase.ExecuteScalar<T>(sql, param, commandTimeout);
            }
            return Conn.ExecuteScalar<T>(sql, param, Tran, CommandTimeout);
        }

        public dynamic QueryFirstOrDefault(string sql, object param = null, int? commandTimeout = null)
        {
            if (Conn == null)
            {
                return DataBase.QueryFirstOrDefault(sql, param, commandTimeout);
            }
            return Conn.QueryFirstOrDefault(sql, param, Tran, commandTimeout: CommandTimeout);
        }

        public T QueryFirstOrDefault<T>(string sql, object param = null, int? commandTimeout = null)
        {
            if (Conn == null)
            {
                return DataBase.QueryFirstOrDefault<T>(sql, param, commandTimeout);
            }
            return Conn.QueryFirstOrDefault<T>(sql, param, Tran, commandTimeout: CommandTimeout);
        }

        public IEnumerable<dynamic> Query(string sql, object param = null, int? commandTimeout = null)
        {
            if (Conn == null)
            {
                return DataBase.Query(sql, param, commandTimeout);
            }
            return Conn.Query(sql, param, Tran, commandTimeout: CommandTimeout);

        }

        public IEnumerable<T> Query<T>(string sql, object param = null, int? commandTimeout = null)
        {
            if (Conn == null)
            {
                return DataBase.Query<T>(sql, param, commandTimeout);
            }
            return Conn.Query<T>(sql, param, Tran, commandTimeout: CommandTimeout);
        }

        public void QueryMultiple(string sql, object param = null, Action<SqlMapper.GridReader> onReader = null, int? commandTimeout = null)
        {
            if (Conn == null)
            {
                DataBase.QueryMultiple(sql, param, onReader, commandTimeout);
                return;
            }
            using (var reader = Conn.QueryMultiple(sql, param, Tran, CommandTimeout))
            {
                onReader?.Invoke(reader);
            }
        }

        #endregion

        #region dapper Async

        public Task<int> ExecuteAsync(string sql, object param = null, int? commandTimeout = null)
        {
            if (Conn == null)
            {
                return DataBase.ExecuteAsync(sql, param, commandTimeout);
            }
            if (DataBase.DbType == DataBaseType.ClickHouse)
            {
                return DataBase.ExecuteAsync(sql, param, commandTimeout);
            }
            return Conn.ExecuteAsync(sql, param, Tran, CommandTimeout);
        }

        public Task<object> ExecuteScalarAsync(string sql, object param = null, int? commandTimeout = null)
        {
            if (Conn == null)
            {
                return DataBase.ExecuteScalarAsync(sql, param, commandTimeout);
            }
            if (DataBase.DbType == DataBaseType.ClickHouse)
            {
                return DataBase.ExecuteScalarAsync(sql, param, commandTimeout);
            }
            return Conn.ExecuteScalarAsync(sql, param, Tran, CommandTimeout);
        }

        public Task<T> ExecuteScalarAsync<T>(string sql, object param = null, int? commandTimeout = null)
        {
            if (Conn == null)
            {
                return DataBase.ExecuteScalarAsync<T>(sql, param, commandTimeout);
            }
            if (DataBase.DbType == DataBaseType.ClickHouse)
            {
                return DataBase.ExecuteScalarAsync<T>(sql, param, commandTimeout);
            }
            return Conn.ExecuteScalarAsync<T>(sql, param, Tran, CommandTimeout);
        }

        public Task<dynamic> QueryFirstOrDefaultAsync(string sql, object param = null, int? commandTimeout = null)
        {
            if (Conn == null)
            {
                return DataBase.QueryFirstOrDefaultAsync(sql, param, commandTimeout);
            }
            if (DataBase.DbType == DataBaseType.ClickHouse)
            {
                return DataBase.QueryFirstOrDefaultAsync(sql, param, commandTimeout);
            }
            return Conn.QueryFirstOrDefaultAsync(sql, param, Tran, commandTimeout: CommandTimeout);

        }

        public Task<T> QueryFirstOrDefaultAsync<T>(string sql, object param = null, int? commandTimeout = null)
        {
            if (Conn == null)
            {
                return DataBase.QueryFirstOrDefaultAsync<T>(sql, param, commandTimeout);
            }
            if (DataBase.DbType == DataBaseType.ClickHouse)
            {
                return DataBase.QueryFirstOrDefaultAsync<T>(sql, param, commandTimeout);
            }
            return Conn.QueryFirstOrDefaultAsync<T>(sql, param, Tran, commandTimeout: CommandTimeout);
        }

        public Task<IEnumerable<dynamic>> QueryAsync(string sql, object param = null, int? commandTimeout = null)
        {
            if (Conn == null)
            {
                return DataBase.QueryAsync(sql, param, commandTimeout);
            }
            if (DataBase.DbType == DataBaseType.ClickHouse)
            {
                return DataBase.QueryAsync(sql, param, commandTimeout);
            }
            return Conn.QueryAsync(sql, param, Tran, commandTimeout: CommandTimeout);
        }

        public Task<IEnumerable<T>> QueryAsync<T>(string sql, object param = null, int? commandTimeout = null)
        {
            if (Conn == null)
            {
                return DataBase.QueryAsync<T>(sql, param, commandTimeout);
            }
            if (DataBase.DbType == DataBaseType.ClickHouse)
            {
                return DataBase.QueryAsync<T>(sql, param, commandTimeout);
            }
            return Conn.QueryAsync<T>(sql, param, Tran, commandTimeout: CommandTimeout);
        }

        public async Task QueryMultipleAsync(string sql, object param = null, Action<SqlMapper.GridReader> onReader = null, int? commandTimeout = null)
        {
            if (Conn == null)
            {
                await DataBase.QueryMultipleAsync(sql, param, onReader, commandTimeout);
                return;
            }
            if (DataBase.DbType == DataBaseType.ClickHouse)
            {
                await DataBase.QueryMultipleAsync(sql, param, onReader, commandTimeout);
                return;
            }
            using (var reader = await Conn.QueryMultipleAsync(sql, param, Tran, CommandTimeout))
            {
                onReader?.Invoke(reader);
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
