using System.Collections.Generic;
using System.Data;
using Z.Dapper.Plus;

namespace Dapper.Sharding
{
    public class DapperEntity
    {
        public DapperEntity(IDatabase database, IDbConnection conn = null, IDbTransaction tran = null, int? commandTimeout = null)
        {
            DataBase = database;
            Conn = conn;
            Tran = tran;
            CommandTimeout = commandTimeout;
        }

        private IDatabase DataBase { get; }

        private IDbConnection Conn { get; }

        private IDbTransaction Tran { get; }

        private int? CommandTimeout { get; }

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

        public void BulkInsert<T>(string tableName, IEnumerable<T> modelList) where T : class
        {
            DapperPlusUtils.Map<T>(tableName);

            if (Conn == null)
            {
                using (var cnn = DataBase.GetConn())
                {
                    cnn.BulkInsert(tableName, modelList);
                }
            }
            else
            {
                Tran.BulkInsert(tableName, modelList);
            }
        }

    }
}
