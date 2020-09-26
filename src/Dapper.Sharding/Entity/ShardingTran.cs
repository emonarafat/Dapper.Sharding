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
            var tb = _sharding.TableList[0];
            keyName = tb.SqlField.PrimaryKey;
            keyType = tb.SqlField.PrimaryKeyType;
        }

        private string keyName { get; }

        private Type keyType { get; }

        private List<IDbConnection> connList = new List<IDbConnection>();

        private List<IDbTransaction> tranList = new List<IDbTransaction>();

        private Dictionary<ITable<T>, ITable<T>> dict = new Dictionary<ITable<T>, ITable<T>>();

        private ISharding<T> _sharding;

        private object _lock = new object();

        private void CreateTranTable(ITable<T> table)
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
                            var tranTable = table.BeginTran(conn, tran);
                            dict.Add(table, tranTable);
                            connList.Add(conn);
                            tranList.Add(tran);
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
                }
            }
        }

        public ITable<T> GetTable(object id)
        {
            var table = _sharding.GetTableById(id);
            CreateTranTable(table);
            return dict[table];
        }

        public ITable<T> GetTable(T model)
        {
            var accessor = TypeAccessor.Create(typeof(T));
            var id = accessor[model, keyName];
            return GetTable(id);
        }

        public ITable<T> GetTableAndInitId(T model)
        {
            var accessor = TypeAccessor.Create(typeof(T));
            var id = accessor[model, keyName];

            if (keyType == typeof(string))
            {
                var key = (string)id;
                if (string.IsNullOrEmpty(key))
                {
                    key = ObjectId.GenerateNewIdAsString();
                    accessor[model, keyName] = key;
                    id = key;
                }
            }
            else if (keyType == typeof(long))
            {
                if ((long)id == 0)
                {
                    var newId = SnowflakeId.GenerateNewId();
                    accessor[model, keyName] = newId;
                    id = newId;
                }
            }
            return GetTable(id);
        }

        public List<ITable<T>> GetTableList()
        {
            foreach (var item in _sharding.TableList)
            {
                CreateTranTable(item);
            }
            return dict.Values.AsList();
        }

        private void Close()
        {
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
            foreach (var tran in tranList)
            {
                tran.Commit();
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
