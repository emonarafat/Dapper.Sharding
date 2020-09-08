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
            return this.Using(() =>
            {
                return this.Execute($"UPDATE `{Name}` SET {SqlField.AllFieldsAtEqExceptKey} WHERE `{SqlField.PrimaryKey}`=@{SqlField.PrimaryKey}", model) > 0;
            });
        }

        public bool UpdateInclude(T model, string fields)
        {
            fields = CommonUtil.GetFieldsAtEqStr(fields.Split(','), "`", "`");
            return this.Using(() =>
            {
                return this.Execute($"UPDATE `{Name}` SET {fields} WHERE `{SqlField.PrimaryKey}`=@{SqlField.PrimaryKey}", model) > 0;
            });
        }

        public bool UpdateExclude(T model, string fields)
        {
            var excludeFields = fields.Split(',').AsEnumerable();
            fields = CommonUtil.GetFieldsAtEqStr(SqlField.AllFieldExceptKeyList.Except(excludeFields), "`", "`");
            return this.Using(() =>
            {
                return this.Execute($"UPDATE `{Name}` SET {fields} WHERE `{SqlField.PrimaryKey}`=@{SqlField.PrimaryKey}", model) > 0;
            });
        }

        public int UpdateByWhere(T model, string where)
        {
            return this.Using(() =>
            {
                return this.Execute($"UPDATE `{Name}` SET {SqlField.AllFieldsAtEqExceptKey} {where}", model);
            });
        }

        public int UpdateByWhereInclude(T model, string where, string fields)
        {
            fields = CommonUtil.GetFieldsAtEqStr(fields.Split(','), "`", "`");
            return this.Using(() =>
            {
                return this.Execute($"UPDATE `{Name}` SET {fields} {where}", model);
            });
        }

        public int UpdateByWhereExclude(T model, string where, string fields)
        {
            var excludeFields = fields.Split(',').AsEnumerable();
            fields = CommonUtil.GetFieldsAtEqStr(SqlField.AllFieldExceptKeyList.Except(excludeFields), "`", "`");
            return this.Using(() =>
            {
                return this.Execute($"UPDATE `{Name}` SET {fields} {where}", model);
            });
        }

        public bool Delete(object id)
        {
            return this.Using(() =>
            {
                return this.Execute($"DELETE FROM `{Name}` WHERE `{SqlField.PrimaryKey}`=@id", new { id }) > 0;
            });
        }

        public bool Delete(T model)
        {
            return this.Using(() =>
            {
                return this.Execute($"DELETE FROM `{Name}` WHERE `{SqlField.PrimaryKey}`=@{SqlField.PrimaryKey}", model) > 0;
            });
        }

        public bool Exists(object id)
        {
            return this.Using(() =>
            {
                return this.ExecuteScalar($"SELECT 1 FROM `{Name}` WHERE `{SqlField.PrimaryKey}`=@id", new { id }) != null;
            });
        }

        public bool Exists(T model)
        {
            return this.Using(() =>
            {
                return this.ExecuteScalar($"SELECT 1 FROM `{Name}` WHERE `{SqlField.PrimaryKey}`=@{SqlField.PrimaryKey}", model) != null;
            });
        }

    }
}
