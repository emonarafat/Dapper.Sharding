using System;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;

namespace Dapper.Sharding
{
    public class DistributedTransaction
    {
        private readonly Dictionary<IDatabase, (IDbConnection, IDbTransaction)> dict = new Dictionary<IDatabase, (IDbConnection, IDbTransaction)>();

        public (IDbConnection, IDbTransaction) GetVal(IDatabase db)
        {
            var ok = dict.TryGetValue(db, out var val);
            if (!ok)
            {
                IDbConnection conn = null;
                IDbTransaction tran = null;
                try
                {
                    conn = db.GetConn();
                    tran = conn.BeginTransaction();
                    val = (conn, tran);
                    dict.Add(db, val);
                }
                catch (Exception ex)
                {
                    if (conn != null)
                    {
                        conn.Dispose();
                    }
                    if (tran != null)
                    {
                        tran.Dispose();
                    }
                    throw ex;
                }
            }
            return val;
        }

        public async Task<(IDbConnection, IDbTransaction)> GetValAsync(IDatabase db)
        {
            var ok = dict.TryGetValue(db, out var val);
            if (!ok)
            {
                IDbConnection conn = null;
                IDbTransaction tran = null;
                try
                {
                    conn = await db.GetConnAsync();
                    tran = conn.BeginTransaction();
                    val = (conn, tran);
                    dict.Add(db, val);
                }
                catch(Exception ex)
                {
                    if (conn != null)
                    {
                        conn.Dispose();
                    }
                    if (tran != null)
                    {
                        tran.Dispose();
                    }
                    throw ex;
                }

            }
            return val;
        }

        private void Close()
        {
            foreach (var item in dict.Values)
            {
                try
                {
                    item.Item1.Dispose();
                    item.Item2.Dispose();
                }
                catch { }
            }
        }

        public void Commit()
        {
            foreach (var item in dict.Values)
            {
                item.Item2.Commit();
            }
            Close();
        }

        public void Rollback()
        {
            foreach (var item in dict.Values)
            {
                try
                {
                    item.Item2.Rollback();
                }
                catch { }
            }
            Close();
        }
    }
}
