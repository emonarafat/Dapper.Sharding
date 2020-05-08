using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dapper.Sharding
{
    internal class MySqlTable<T> : ITable<T>
    {
        public MySqlTable(string name, IDbConnection conn, IDbTransaction tran = null, int? commandTimeout = null)
        {
            Name = name;
            Conn = conn;
            Tran = tran;
            CommandTimeout = commandTimeout;
        }

        public IDbConnection Conn { get; }

        public IDbTransaction Tran { get; }

        public int? CommandTimeout { get; }

        public string Name { get; }

        public int Insert(T model)
        {
            return 0;
        }

        public int Update(T model)
        {
            throw new NotImplementedException();
        }
    }
}
