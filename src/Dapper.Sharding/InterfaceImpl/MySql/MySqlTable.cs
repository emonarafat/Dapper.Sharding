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
        public MySqlTable(string name, IDatabase database, IDbConnection conn = null, IDbTransaction tran = null, int? commandTimeout = null)
        {
            Name = name;
            DataBase = database;
            Conn = conn;
            Tran = tran;
            CommandTimeout = commandTimeout;
        }

        public IDbConnection Conn { get; set; }

        public IDbTransaction Tran { get; }

        public int? CommandTimeout { get; }

        public string Name { get; }

        public IDatabase DataBase { get; }

        public int Insert(T model)
        {
            this.Using(() =>
            {

            });
           return 0;
        }

        public int Update(T model)
        {
            return this.Using(() =>
            {
                return 1;
            });
            //throw new NotImplementedException();
        }
    }
}
