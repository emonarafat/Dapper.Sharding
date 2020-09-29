using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace Dapper.Sharding
{
    public class DistributedTransaction
    {
        private List<IDbConnection> connList = new List<IDbConnection>();

        private List<IDbTransaction> tranList = new List<IDbTransaction>();

        private Dictionary<object, object> dict = new Dictionary<object, object>();

        private object _lock = new object();

        public int? CommandTimeout = null;

        public ITable<T> GetTranTable<T>(ITable<T> table)
        {
            if (!dict.ContainsKey(table))
            {
                lock (_lock)
                {
                    if (!dict.ContainsKey(table))
                    {
                        IDbConnection conn = null;
                        IDbTransaction tran = null;
                        try
                        {
                            conn = table.DataBase.GetConn();
                            tran = conn.BeginTransaction();
                            ITable<T> tranTable = null; //table.BeginTran(conn, tran);
                            dict.Add(table, tranTable);
                            connList.Add(conn);
                            tranList.Add(tran);
                        }
                        catch (Exception ex)
                        {
                            if (conn != null && conn.State == ConnectionState.Open)
                            {
                                conn.Dispose();
                            }
                            if (tran != null)
                            {
                                tran.Dispose();
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
            foreach (var item in tranList)
            {
                try
                {
                    item.Dispose();
                }
                catch { }
            }

            foreach (var item in connList)
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
            foreach (var item in tranList)
            {
                item.Commit();
            }
            Close();
        }

        public void Rollback()
        {
            foreach (var item in tranList)
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
