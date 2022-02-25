using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

namespace Dapper.Sharding
{
    public class DistributedTransaction
    {
        private IDatabase defaultDb = null;
        private (IDbConnection, IDbTransaction) defaultVal = default;
        private Dictionary<IDatabase, (IDbConnection, IDbTransaction)> dict = null;

        private (IDbConnection, IDbTransaction) CreateConnAndTran(IDatabase db)
        {
            IDbConnection conn = null;
            IDbTransaction tran = null;
            try
            {
                conn = db.GetConn();
                tran = conn.BeginTransaction();
                return (conn, tran);
            }
            catch
            {
                if (tran != null)
                {
                    tran.Dispose();
                }
                if (conn != null)
                {
                    conn.Dispose();
                }
                throw;
            }
        }

        private async Task<(IDbConnection, IDbTransaction)> CreateConnAndTranAsync(IDatabase db)
        {
            IDbConnection conn = null;
            IDbTransaction tran = null;
            try
            {
                conn = await db.GetConnAsync();
                tran = conn.BeginTransaction();
                return (conn, tran);
            }
            catch
            {
                if (tran != null)
                {
                    tran.Dispose();
                }
                if (conn != null)
                {
                    conn.Dispose();
                }
                throw;
            }

        }


        public (IDbConnection, IDbTransaction) GetVal(IDatabase db)
        {
            if (defaultDb == null) //第一次初始化
            {
                IDbConnection conn = null;
                IDbTransaction tran = null;
                try
                {
                    conn = db.GetConn();
                    tran = conn.BeginTransaction();
                }
                catch
                {
                    if (tran != null)
                    {
                        tran.Dispose();
                    }
                    if (conn != null)
                    {
                        conn.Dispose();
                    }
                    throw;
                }
                defaultDb = db;
                defaultVal.Item1 = conn;
                defaultVal.Item2 = tran;
                return defaultVal;
            }
            else if (defaultDb.Equals(db))
            {
                return defaultVal;
            }
            else
            {
                if (dict == null)//dict为null第一次创建
                {
                    dict = new Dictionary<IDatabase, (IDbConnection, IDbTransaction)>();
                    var val = CreateConnAndTran(db);
                    dict.Add(db, val);
                    return val;
                }
                else
                {
                    var ok = dict.TryGetValue(db, out var val);
                    if (!ok)
                    {
                        val = CreateConnAndTran(db);
                        dict.Add(db, val);
                    }
                    return val;
                }
            }
        }

        public async Task<(IDbConnection, IDbTransaction)> GetValAsync(IDatabase db)
        {
            if (defaultDb == null) //第一次初始化
            {
                IDbConnection conn = null;
                IDbTransaction tran = null;
                try
                {
                    conn = await db.GetConnAsync();
                    tran = conn.BeginTransaction();
                }
                catch
                {
                    if (tran != null)
                    {
                        tran.Dispose();
                    }
                    if (conn != null)
                    {
                        conn.Dispose();
                    }
                    throw;
                }
                defaultDb = db;
                defaultVal.Item1 = conn;
                defaultVal.Item2 = tran;
                return defaultVal;
            }
            else if (defaultDb.Equals(db))
            {
                return defaultVal;
            }
            else
            {
                if (dict == null)//dict为null第一次创建
                {
                    dict = new Dictionary<IDatabase, (IDbConnection, IDbTransaction)>();
                    var val = await CreateConnAndTranAsync(db);
                    dict.Add(db, val);
                    return val;
                }
                else
                {
                    var ok = dict.TryGetValue(db, out var val);
                    if (!ok)
                    {
                        val = await CreateConnAndTranAsync(db);
                        dict.Add(db, val);
                    }
                    return val;
                }
            }
        }

        public void Commit()
        {
            if (defaultDb != null)
            {
                try
                {
                    defaultVal.Item2.Commit();
                }
                catch
                {
                    throw; //如果第一个提交失败，抛出异常执行Rollback全部回滚
                }
                finally
                {
                    defaultVal.Item2.Dispose();
                    defaultVal.Item1.Dispose();
                }
                defaultDb = null;
            }
            if (dict != null && dict.Count > 0)
            {
                foreach (var item in dict.Values)
                {
                    try
                    {
                        item.Item2.Commit();
                    }
                    catch
                    {
                        try
                        {
                            item.Item2.Rollback();
                        }
                        catch { }
                    }
                    finally
                    {
                        item.Item2.Dispose();
                        item.Item1.Dispose();
                    }
                }
                dict.Clear();
            }
        }

        public void Rollback()
        {
            if (defaultDb != null)
            {
                try
                {
                    defaultVal.Item2.Rollback();
                }
                finally
                {
                    defaultVal.Item2.Dispose();
                    defaultVal.Item1.Dispose();
                }
                defaultDb = null;
            }
            if (dict != null && dict.Count > 0)
            {
                foreach (var item in dict.Values)
                {
                    try
                    {
                        item.Item2.Rollback();
                    }
                    finally
                    {
                        item.Item2.Dispose();
                        item.Item1.Dispose();
                    }
                }
                dict.Clear();
            }
        }
    }
}
