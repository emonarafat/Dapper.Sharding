﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
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
                var conn = db.GetConn();
                var tran = conn.BeginTransaction();
                val = (conn, tran);
                dict.Add(db, val);
            }
            return val;
        }

        public async Task<(IDbConnection, IDbTransaction)> GetValAsync(IDatabase db)
        {
            var ok = dict.TryGetValue(db, out var val);
            if (!ok)
            {
                IDbConnection conn = await db.GetConnAsync();
                IDbTransaction tran = conn.BeginTransaction();
                val = (conn, tran);
                dict.Add(db, val);
            }
            return val;
        }

        public void Commit()
        {
            if (dict.Count == 1)
            {
                var item = dict.First().Value;
                item.Item2.Commit();
                item.Item2.Dispose();
                item.Item1.Dispose();
                return;
            }
            foreach (var item in dict.Values)
            {
                item.Item2.Commit();
            }
            foreach (var item in dict.Values)
            {
                item.Item2.Dispose();
                item.Item1.Dispose();
            }
        }

        public void Rollback()
        {
            if (dict.Count == 1)
            {
                var item = dict.First().Value;
                item.Item2.Rollback();
                item.Item2.Dispose();
                item.Item1.Dispose();
                return;
            }

            foreach (var item in dict.Values)
            {
                try
                {
                    item.Item2.Rollback();
                }
                catch
                {

                }
                finally
                {
                    item.Item2.Dispose();
                    item.Item1.Dispose();
                }
            }
        }
    }
}
