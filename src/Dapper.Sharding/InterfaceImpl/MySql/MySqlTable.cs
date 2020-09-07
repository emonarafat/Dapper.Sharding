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

        public IDbTransaction Tran { get; set; }

        public int? CommandTimeout { get; set; }

        public string Name { get; }

        public IDatabase DataBase { get; }

        public bool Insert(T model)
        {
            return this.Using(() =>
            {
                return this.Execute("INSERT INTO People(Name)VALUES('小哦哦奥')", model) > 0;
            });
        }

        public bool Update(T model)
        {
            return this.Using(() =>
            {
                return false;
            });
            //throw new NotImplementedException();
        }

        public int UpdateByWhere(T model, string where, string updateFields)
        {
            throw new NotImplementedException();
        }

        public bool Delete(object id)
        {
            throw new NotImplementedException();
        }

        public bool Delete(T model)
        {
            throw new NotImplementedException();
        }


    }
}
