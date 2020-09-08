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

        public ITable<T> BeginTran(IDbConnection conn, IDbTransaction tran, int? commandTimeout = null)
        {
            return new MySqlTable<T>(Name, DataBase, conn, tran, commandTimeout);
        }

        public IDbConnection Conn { get; }

        public IDbTransaction Tran { get; }

        public int? CommandTimeout { get; }

        public string Name { get; }

        public IDatabase DataBase { get; }

        public SqlFieldEntity SqlField { get; } = SqlFieldCacheUtils.GetMySqlFieldEntity<T>();

        public bool Insert(T model)
        {
            var accessor = TypeAccessor.Create(typeof(T));
            if (SqlField.IsIdentity)
            {
                var sql = $"INSERT INTO `{Name}` ({SqlField.AllFieldsExceptKey})VALUES({SqlField.AllFieldsAtExceptKey});SELECT @@IDENTITY";
                if (SqlField.PrimaryKeyType == typeof(int))
                {
                    var id = this.ExecuteScalar<T, int>(sql, model);
                    accessor[model, SqlField.PrimaryKey] = id;
                    return id > 0;
                }
                else
                {
                    var id = this.ExecuteScalar<T, long>(sql, model);
                    accessor[model, SqlField.PrimaryKey] = id;
                    return id > 0;
                }
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
        }

        public bool InsertIdentity(T model)
        {
            return this.Execute($"INSERT INTO `{Name}` ({SqlField.AllFields})VALUES({SqlField.AllFieldsAt})", model) > 0;
        }

        public bool InsertIfNoExists(T model)
        {
            if (!Exists(model))
            {
                return Insert(model);
            }
            return false;
        }

        public bool InsertIfExistsUpdate(T model, string fields = null)
        {
            if (Exists(model))
            {
                if (fields == null)
                    return Update(model);
                else
                    return UpdateInclude(model, fields);
            }
            else
            {
                return Insert(model);
            }
        }

        public bool InsertIdentityIfNoExists(T model)
        {
            if (!Exists(model))
            {
                return InsertIdentity(model);
            }
            return false;
        }

        public bool InsertIdentityIfExistsUpdate(T model, string fields = null)
        {
            if (Exists(model))
            {
                if (fields == null)
                    return Update(model);
                else
                    return UpdateInclude(model, fields);
            }
            else
            {
                return InsertIdentity(model);
            }
        }

        public bool Update(T model)
        {
            return this.Execute($"UPDATE `{Name}` SET {SqlField.AllFieldsAtEqExceptKey} WHERE `{SqlField.PrimaryKey}`=@{SqlField.PrimaryKey}", model) > 0;
        }

        public bool UpdateInclude(T model, string fields)
        {
            fields = CommonUtil.GetFieldsAtEqStr(fields.Split(','), "`", "`");
            return this.Execute($"UPDATE `{Name}` SET {fields} WHERE `{SqlField.PrimaryKey}`=@{SqlField.PrimaryKey}", model) > 0;
        }

        public bool UpdateExclude(T model, string fields)
        {
            var excludeFields = fields.Split(',').AsEnumerable();
            fields = CommonUtil.GetFieldsAtEqStr(SqlField.AllFieldExceptKeyList.Except(excludeFields), "`", "`");
            return this.Execute($"UPDATE `{Name}` SET {fields} WHERE `{SqlField.PrimaryKey}`=@{SqlField.PrimaryKey}", model) > 0;
        }

        public int UpdateByWhere(T model, string where)
        {
            return this.Execute($"UPDATE `{Name}` SET {SqlField.AllFieldsAtEqExceptKey} {where}", model);
        }

        public int UpdateByWhereInclude(T model, string where, string fields)
        {
            fields = CommonUtil.GetFieldsAtEqStr(fields.Split(','), "`", "`");
            return this.Execute($"UPDATE `{Name}` SET {fields} {where}", model);
        }

        public int UpdateByWhereExclude(T model, string where, string fields)
        {
            var excludeFields = fields.Split(',').AsEnumerable();
            fields = CommonUtil.GetFieldsAtEqStr(SqlField.AllFieldExceptKeyList.Except(excludeFields), "`", "`");
            return this.Execute($"UPDATE `{Name}` SET {fields} {where}", model);
        }

        public bool Delete(object id)
        {
            return this.Execute($"DELETE FROM `{Name}` WHERE `{SqlField.PrimaryKey}`=@id", new { id }) > 0;
        }

        public bool Delete(T model)
        {
            return this.Execute($"DELETE FROM `{Name}` WHERE `{SqlField.PrimaryKey}`=@{SqlField.PrimaryKey}", model) > 0;
        }

        public bool Exists(object id)
        {
            return this.ExecuteScalar($"SELECT 1 FROM `{Name}` WHERE `{SqlField.PrimaryKey}`=@id", new { id }) != null;
        }

        public bool Exists(T model)
        {
            return this.ExecuteScalar($"SELECT 1 FROM `{Name}` WHERE `{SqlField.PrimaryKey}`=@{SqlField.PrimaryKey}", model) != null;
        }
    }
}
