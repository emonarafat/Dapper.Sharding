using System;
using System.Collections.Generic;
using System.Data;

namespace Dapper.Sharding
{
    public class DistributedTransaction
    {
        private Dictionary<IDatabase, IDbConnection> dictConn = new Dictionary<IDatabase, IDbConnection>();

        private Dictionary<IDatabase, IDbTransaction> dictTran = new Dictionary<IDatabase, IDbTransaction>();

        private Dictionary<object, object> dict = new Dictionary<object, object>();

        private object _lock = new object();

        public int? CommandTimeout = null;

        public ITable<T> GetTranTable<T>(ITable<T> table) where T : class
        {
            if (!dict.ContainsKey(table))
            {
                lock (_lock)
                {
                    if (!dict.ContainsKey(table))
                    {
                        #region database conn tran cache

                        var db = table.DataBase;
                        IDbConnection conn = null;
                        IDbTransaction tran = null;

                        //不存在此db的conn进行添加
                        if (!dictConn.ContainsKey(db))
                        {
                            conn = db.GetConn();
                            tran = conn.BeginTransaction();
                            dictConn.Add(db, conn);
                            dictTran.Add(db, tran);
                        }
                        else //直接取出来
                        {
                            conn = dictConn[db];
                            tran = dictTran[db];
                        }

                        #endregion

                        try
                        {
                            ITable<T> tranTable = table.CreateTranTable(conn, tran, CommandTimeout);
                            dict.Add(table, tranTable);
                        }
                        catch (Exception ex)
                        {
                            if (conn != null && conn.State == ConnectionState.Open)
                            {
                                try
                                {
                                    conn.Dispose();
                                }
                                catch { }

                            }
                            if (tran != null)
                            {
                                try
                                {
                                    tran.Dispose();
                                }
                                catch { }

                            }
                            Close();
                            throw ex;
                        }
                    }
                }
            }
            return (ITable<T>)dict[table];
        }

        private void Close()
        {
            foreach (var item in dictTran.Values)
            {
                try
                {
                    item.Dispose();
                }
                catch { }
            }

            foreach (var item in dictConn.Values)
            {
                try
                {
                    item.Dispose();
                }
                catch { }
            }
        }

        public void Commit()
        {
            foreach (var item in dictTran.Values)
            {
                item.Commit();
            }
            Close();
        }

        public void Rollback()
        {
            foreach (var item in dictTran.Values)
            {
                try
                {
                    item.Rollback();
                }
                catch { }
            }
            Close();
        }
    }
}
