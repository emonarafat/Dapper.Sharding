using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

namespace Dapper.Sharding
{
    internal class MySqlTable<T> : ITable<T> where T : class
    {
        public MySqlTable(string name, IDatabase database, IDbConnection conn = null, IDbTransaction tran = null, int? commandTimeout = null)
        {
            Name = name;
            DataBase = database;
            SqlField = SqlFieldCacheUtils.GetMySqlFieldEntity<T>();
            DpEntity = new DapperEntity(database, conn, tran, commandTimeout);
        }

        public string Name { get; }

        public IDatabase DataBase { get; }

        public SqlFieldEntity SqlField { get; }

        public DapperEntity DpEntity { get; }

        public ITable<T> CreateTranTable(IDbConnection conn, IDbTransaction tran, int? commandTimeout = null)
        {
            return new MySqlTable<T>(Name, DataBase, conn, tran, commandTimeout);
        }

        public bool Insert(T model)
        {
            var accessor = TypeAccessor.Create(typeof(T));
            if (SqlField.IsIdentity)
            {
                var sql = $"INSERT INTO `{Name}` ({SqlField.AllFieldsExceptKey})VALUES({SqlField.AllFieldsAtExceptKey});SELECT @@IDENTITY";
                if (SqlField.PrimaryKeyType == typeof(int))
                {
                    var id = DpEntity.ExecuteScalar<int>(sql, model);
                    accessor[model, SqlField.PrimaryKey] = id;
                    return id > 0;
                }
                else
                {
                    var id = DpEntity.ExecuteScalar<long>(sql, model);
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
                else if (SqlField.PrimaryKeyType == typeof(long))
                {
                    var val = (long)accessor[model, SqlField.PrimaryKey];
                    if (val == 0)
                    {
                        accessor[model, SqlField.PrimaryKey] = SnowflakeId.GenerateNewId();
                    }
                }
                var sql = $"INSERT INTO `{Name}` ({SqlField.AllFields})VALUES({SqlField.AllFieldsAt})";
                return DpEntity.Execute(sql, model) > 0;
            }
        }

        public void BulkInsert(IEnumerable<T> modelList)
        {
            DpEntity.BulkInsert(Name, modelList);
        }

        public bool InsertIdentity(T model)
        {
            return DpEntity.Execute($"INSERT INTO `{Name}` ({SqlField.AllFields})VALUES({SqlField.AllFieldsAt})", model) > 0;
        }

        public int InsertIdentityMany(IEnumerable<T> modelList)
        {
            return DpEntity.Execute($"INSERT INTO `{Name}` ({SqlField.AllFields})VALUES({SqlField.AllFieldsAt})", modelList);
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
            return DpEntity.Execute($"UPDATE `{Name}` SET {SqlField.AllFieldsAtEqExceptKey} WHERE `{SqlField.PrimaryKey}`=@{SqlField.PrimaryKey}", model) > 0;
        }

        public int UpdateMany(IEnumerable<T> modelList)
        {
            return DpEntity.Execute($"UPDATE `{Name}` SET {SqlField.AllFieldsAtEqExceptKey} WHERE `{SqlField.PrimaryKey}`=@{SqlField.PrimaryKey}", modelList);
        }

        public bool UpdateInclude(T model, string fields)
        {
            fields = CommonUtil.GetFieldsAtEqStr(fields.Split(','), "`", "`");
            return DpEntity.Execute($"UPDATE `{Name}` SET {fields} WHERE `{SqlField.PrimaryKey}`=@{SqlField.PrimaryKey}", model) > 0;
        }

        public int UpdateIncludeMany(IEnumerable<T> modelList, string fields)
        {
            fields = CommonUtil.GetFieldsAtEqStr(fields.Split(','), "`", "`");
            return DpEntity.Execute($"UPDATE `{Name}` SET {fields} WHERE `{SqlField.PrimaryKey}`=@{SqlField.PrimaryKey}", modelList);
        }

        public bool UpdateExclude(T model, string fields)
        {
            var excludeFields = fields.Split(',').AsEnumerable();
            fields = CommonUtil.GetFieldsAtEqStr(SqlField.AllFieldExceptKeyList.Except(excludeFields), "`", "`");
            return DpEntity.Execute($"UPDATE `{Name}` SET {fields} WHERE `{SqlField.PrimaryKey}`=@{SqlField.PrimaryKey}", model) > 0;
        }

        public int UpdateExcludeMany(IEnumerable<T> modelList, string fields)
        {
            var excludeFields = fields.Split(',').AsEnumerable();
            fields = CommonUtil.GetFieldsAtEqStr(SqlField.AllFieldExceptKeyList.Except(excludeFields), "`", "`");
            return DpEntity.Execute($"UPDATE `{Name}` SET {fields} WHERE `{SqlField.PrimaryKey}`=@{SqlField.PrimaryKey}", modelList);
        }

        public int UpdateByWhere(T model, string where)
        {
            return DpEntity.Execute($"UPDATE `{Name}` SET {SqlField.AllFieldsAtEqExceptKey} {where}", model);
        }

        public int UpdateByWhereInclude(T model, string where, string fields)
        {
            fields = CommonUtil.GetFieldsAtEqStr(fields.Split(','), "`", "`");
            return DpEntity.Execute($"UPDATE `{Name}` SET {fields} {where}", model);
        }

        public int UpdateByWhereExclude(T model, string where, string fields)
        {
            var excludeFields = fields.Split(',').AsEnumerable();
            fields = CommonUtil.GetFieldsAtEqStr(SqlField.AllFieldExceptKeyList.Except(excludeFields), "`", "`");
            return DpEntity.Execute($"UPDATE `{Name}` SET {fields} {where}", model);
        }

        public bool Delete(object id)
        {
            return DpEntity.Execute($"DELETE FROM `{Name}` WHERE `{SqlField.PrimaryKey}`=@id", new { id }) > 0;
        }

        public int DeleteByIds(object ids)
        {
            if (CommonUtil.ObjectIsEmpty(ids))
                return 0;
            var dpar = new DynamicParameters();
            dpar.Add("@ids", ids);
            return DpEntity.Execute($"DELETE FROM `{Name}` WHERE `{SqlField.PrimaryKey}` IN @ids", dpar);
        }

        public int DeleteByWhere(string where, object param)
        {
            return DpEntity.Execute($"DELETE FROM `{Name}` " + where, param);
        }

        public int DeleteAll()
        {
            return DpEntity.Execute($"DELETE FROM `{Name}`");
        }

        public void Truncate()
        {
            DataBase.TruncateTable(Name);
        }

        public bool Exists(object id)
        {
            return DpEntity.ExecuteScalar($"SELECT 1 FROM `{Name}` WHERE `{SqlField.PrimaryKey}`=@id", new { id }) != null;
        }

        public long Count()
        {
            return DpEntity.ExecuteScalar<long>($"SELECT COUNT(1) FROM `{Name}`");
        }

        public long Count(string where, object param = null)
        {
            return DpEntity.ExecuteScalar<long>($"SELECT COUNT(1) FROM `{Name}` {where}", param);
        }

        public TValue Min<TValue>(string field, string where = null, object param = null)
        {
            return DpEntity.ExecuteScalar<TValue>($"SELECT MIN(`{field}`) FROM `{Name}` {where}", param);
        }

        public TValue Max<TValue>(string field, string where = null, object param = null)
        {
            return DpEntity.ExecuteScalar<TValue>($"SELECT MAX(`{field}`) FROM `{Name}` {where}", param);
        }

        public TValue Sum<TValue>(string field, string where = null, object param = null)
        {
            return DpEntity.ExecuteScalar<TValue>($"SELECT SUM(`{field}`) FROM `{Name}` {where}", param);
        }

        public TValue Avg<TValue>(string field, string where = null, object param = null)
        {
            return DpEntity.ExecuteScalar<TValue>($"SELECT AVG(`{field}`) FROM `{Name}` {where}", param);
        }

        public IEnumerable<T> GetAll(string returnFields = null, string orderby = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return DpEntity.Query<T>($"SELECT {returnFields} FROM `{Name}` {orderby.SetOrderBy(SqlField.PrimaryKey)}");
        }

        public T GetById(object id, string returnFields = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return DpEntity.QueryFirstOrDefault<T>($"SELECT {returnFields} FROM `{Name}` WHERE `{SqlField.PrimaryKey}`=@id", new { id });
        }

        public T GetByIdForUpdate(object id, string returnFields = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return DpEntity.QueryFirstOrDefault<T>($"SELECT {returnFields} FROM `{Name}` WHERE `{SqlField.PrimaryKey}`=@id FOR UPDATE", new { id });
        }

        public IEnumerable<T> GetByIds(object ids, string returnFields = null)
        {
            if (CommonUtil.ObjectIsEmpty(ids))
                return Enumerable.Empty<T>();
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            var dpar = new DynamicParameters();
            dpar.Add("@ids", ids);
            return DpEntity.Query<T>($"SELECT {returnFields} FROM `{Name}` WHERE `{SqlField.PrimaryKey}` IN @ids", dpar);
        }

        public IEnumerable<T> GetByIdsForUpdate(object ids, string returnFields = null)
        {
            if (CommonUtil.ObjectIsEmpty(ids))
                return Enumerable.Empty<T>();
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            var dpar = new DynamicParameters();
            dpar.Add("@ids", ids);
            return DpEntity.Query<T>($"SELECT {returnFields} FROM `{Name}` WHERE `{SqlField.PrimaryKey}` IN @ids FOR UPDATE", dpar);
        }

        public IEnumerable<T> GetByIdsWithField(object ids, string field, string returnFields = null)
        {
            if (CommonUtil.ObjectIsEmpty(ids))
                return Enumerable.Empty<T>();
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            var dpar = new DynamicParameters();
            dpar.Add("@ids", ids);
            return DpEntity.Query<T>($"SELECT {returnFields} FROM `{Name}` WHERE `{field}` IN @ids", dpar);
        }

        public IEnumerable<T> GetByWhere(string where, object param = null, string returnFields = null, string orderby = null, int limit = 0)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            string limitStr = null;
            if (limit != 0)
            {
                limitStr = "LIMIT " + limit;
            }
            return DpEntity.Query<T>($"SELECT {returnFields} FROM `{Name}` {where} {orderby.SetOrderBy(SqlField.PrimaryKey)} {limitStr}", param);
        }

        public T GetByWhereFirst(string where, object param = null, string returnFields = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return DpEntity.QueryFirstOrDefault<T>($"SELECT {returnFields} FROM `{Name}` {where} LIMIT 1", param);
        }

        public IEnumerable<T> GetBySkipTake(int skip, int take, string where = null, object param = null, string returnFields = null, string orderby = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return DpEntity.Query<T>($"SELECT {returnFields} FROM `{Name}` {where} {orderby.SetOrderBy(SqlField.PrimaryKey)} LIMIT {skip},{take}", param);
        }

        public IEnumerable<T> GetByPage(int page, int pageSize, string where = null, object param = null, string returnFields = null, string orderby = null)
        {
            int skip = 0;
            if (page > 0)
            {
                skip = (page - 1) * pageSize;
            }
            return GetBySkipTake(skip, pageSize, where, param, returnFields, orderby);
        }

        public IEnumerable<T> GetByPageAndCount(int page, int pageSize, out long count, string where = null, object param = null, string returnFields = null, string orderby = null)
        {
            var task1 = Task.Run(() =>
            {
                return Count(where, param);
            });
            var task2 = Task.Run(() =>
            {
                return GetByPage(page, pageSize, where, param, returnFields, orderby);
            });
            Task.WhenAll(task1, task2).Wait();
            count = task1.Result;
            return task2.Result;
        }

        public IEnumerable<T> GetByAscFirstPage(int pageSize, object param = null, string and = null, string returnFields = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return DpEntity.Query<T>($"SELECT {returnFields} FROM `{Name}` AS A WHERE 1=1 {and} ORDER BY `{SqlField.PrimaryKey}` LIMIT {pageSize}", param);
        }

        public IEnumerable<T> GetByAscPrevPage(int pageSize, T param, string and = null, string returnFields = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return DpEntity.Query<T>($"SELECT * FROM (SELECT {returnFields} FROM `{Name}` AS A WHERE `{SqlField.PrimaryKey}`<@{SqlField.PrimaryKey} {and} ORDER BY `{SqlField.PrimaryKey}` DESC LIMIT {pageSize}) AS B ORDER BY `{SqlField.PrimaryKey}`", param);
        }

        public IEnumerable<T> GetByAscCurrentPage(int pageSize, T param, string and = null, string returnFields = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return DpEntity.Query<T>($"SELECT {returnFields} FROM `{Name}` AS A WHERE `{SqlField.PrimaryKey}`>=@{SqlField.PrimaryKey} {and} ORDER BY `{SqlField.PrimaryKey}` LIMIT {pageSize}", param);
        }

        public IEnumerable<T> GetByAscNextPage(int pageSize, T param, string and = null, string returnFields = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return DpEntity.Query<T>($"SELECT {returnFields} FROM `{Name}` AS A WHERE `{SqlField.PrimaryKey}`>@{SqlField.PrimaryKey} {and} ORDER BY `{SqlField.PrimaryKey}` LIMIT {pageSize}", param);
        }

        public IEnumerable<T> GetByAscLastPage(int pageSize, object param = null, string and = null, string returnFields = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return DpEntity.Query<T>($"SELECT * FROM (SELECT {returnFields} FROM `{Name}` AS A WHERE 1=1 {and} ORDER BY `{SqlField.PrimaryKey}` DESC LIMIT {pageSize}) AS B ORDER BY `{SqlField.PrimaryKey}`", param);
        }

        public IEnumerable<T> GetByDescFirstPage(int pageSize, object param = null, string and = null, string returnFields = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return DpEntity.Query<T>($"SELECT {returnFields} FROM `{Name}` AS A WHERE 1=1 {and} ORDER BY `{SqlField.PrimaryKey}` DESC LIMIT {pageSize}", param);
        }

        public IEnumerable<T> GetByDescPrevPage(int pageSize, T param, string and = null, string returnFields = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return DpEntity.Query<T>($"SELECT * FROM (SELECT {returnFields} FROM `{Name}` AS A WHERE `{SqlField.PrimaryKey}`>@{SqlField.PrimaryKey} {and} ORDER BY `{SqlField.PrimaryKey}` LIMIT {pageSize}) AS B ORDER BY `{SqlField.PrimaryKey}` DESC", param);
        }

        public IEnumerable<T> GetByDescCurrentPage(int pageSize, T param, string and = null, string returnFields = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return DpEntity.Query<T>($"SELECT {returnFields} FROM `{Name}` AS A WHERE `{SqlField.PrimaryKey}`<=@{SqlField.PrimaryKey} {and} ORDER BY `{SqlField.PrimaryKey}` DESC LIMIT {pageSize}", param);
        }

        public IEnumerable<T> GetByDescNextPage(int pageSize, T param, string and = null, string returnFields = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return DpEntity.Query<T>($"SELECT {returnFields} FROM `{Name}` AS A WHERE `{SqlField.PrimaryKey}`<@{SqlField.PrimaryKey} {and} ORDER BY `{SqlField.PrimaryKey}` DESC LIMIT {pageSize}", param);
        }

        public IEnumerable<T> GetByDescLastPage(int pageSize, object param = null, string and = null, string returnFields = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return DpEntity.Query<T>($"SELECT * FROM (SELECT {returnFields} FROM `{Name}` AS A WHERE 1=1 {and} ORDER BY `{SqlField.PrimaryKey}` LIMIT {pageSize}) AS B ORDER BY `{SqlField.PrimaryKey}` DESC", param);
        }
    }
}
