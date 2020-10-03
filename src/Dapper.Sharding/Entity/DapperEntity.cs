using System;
using System.Collections.Generic;
using System.Data;
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
