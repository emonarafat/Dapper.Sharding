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

        public SqlFieldEntity SqlField { get; } = SqlFieldCacheUtils.GetMySqlFieldEntity<T>();

        public bool Insert(T model)
        {
            return this.Using(() =>
            {
                var accessor = TypeAccessor.Create(typeof(T));
                if (SqlField.IsIdentity)
                {
                    var sql = $"INSERT INTO `{Name}` ({SqlField.AllFieldsExceptKey})VALUES({SqlField.AllFieldsAtExceptKey})";
                    var res = this.Execute(sql, model);
                    if (res > 0)
                    {
                        if (SqlField.PrimaryKeyType == typeof(int))
                        {
                            accessor[model, SqlField.PrimaryKey] = this.ExecuteScalar<T, int>("SELECT @@IDENTITY");
                        }
                        else
                        {
                            accessor[model, SqlField.PrimaryKey] = this.ExecuteScalar<T, long>("SELECT @@IDENTITY");
                        }
                    }
                    return res > 0;
                }
                else
                {
                    if (SqlField.PrimaryKeyType == typeof(string))
                    {
                        var val = (string)accessor[model, SqlField.PrimaryKey];
                        if (string.IsNullOrEmpty(val))
                        {
                            accessor[model, SqlField.PrimaryKey] = ObjectId.GenerateNewIdAsString();
                        }
                    }
                    var sql = $"INSERT INTO `{Name}` ({SqlField.AllFields})VALUES({SqlField.AllFieldsAt})";
                    return this.Execute(sql, model) > 0;
                }

            });
        }

        public bool InsertIdentity(T model)
        {
            return this.Using(() =>
            {
                return this.Execute($"INSERT INTO `{Name}` ({SqlField.AllFields})VALUES({SqlField.AllFieldsAt})", model) > 0;
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
