using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dapper.Sharding
{
    public class ShardingTran<T>
    {

        public ShardingTran(ISharding<T> sharding)
        {
            _sharding = sharding;
        }

        private Dictionary<object, ShardingTranEntity<T>> dict = new Dictionary<object, ShardingTranEntity<T>>();

        private ISharding<T> _sharding;

        private object _lock = new object();

        public ITable<T> GetTable(object id)
        {

            if (dict.ContainsKey(id))
            {
                return dict[id].Table;
            }

            if (!dict.ContainsKey(id))
            {
                var table = _sharding.GetTableById(id);
                IDbConnection conn = null;
                IDbTransaction tran = null;
                try
                {
                    conn = table.DataBase.GetConn();
                    tran = conn.BeginTransaction();
                    var tranTable = table.BeginTran(conn, tran);
                    dict.Add(id, new ShardingTranEntity<T> { Conn = conn, Tran = tran, Table = tranTable });
                    return tranTable;
                }
                catch
                {
                    if (conn != null && conn.State == ConnectionState.Open)
                    {
                        conn.Close();
                    }
                    if (tran != null)
                    {
                        tran.Dispose();
                    }
                    Close();
                }
            }
            throw new Exception("create tran table error");
        }

        private void Close()
        {
            var connList = dict.Values.Select(s => s.Conn);
            foreach (var conn in connList)
            {
                try
                {
                    conn.Close();
                }
                catch { }
            }
        }

        public void Commit()
        {
            var tranList = dict.Values.Select(s => s.Tran);
            foreach (var tran in tranList)
            {
                tran.Commit();
            }
            Close();
        }

        public void Rollback()
        {
            var tranList = dict.Values.Select(s => s.Tran);
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
