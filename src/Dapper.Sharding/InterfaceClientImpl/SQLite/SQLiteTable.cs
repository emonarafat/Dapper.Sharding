using FastMember;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace Dapper.Sharding
{
    internal partial class SQLiteTable<T> : ITable<T> where T : class
    {
        public SQLiteTable(string name, IDatabase database) : base(name, database, SqlFieldCacheUtils.GetSqliteFieldEntity<T>())
        {

        }

        #region insert

        protected override string SqlInsert()
        {
            if (SqlField.IsIdentity)
            {
                return $"INSERT INTO [{Name}] ({SqlField.AllFieldsExceptKey})VALUES({SqlField.AllFieldsAtExceptKey});SELECT LAST_INSERT_ROWID()";
            }
            else
            {
                return $"INSERT INTO [{Name}] ({SqlField.AllFields})VALUES({SqlField.AllFieldsAt})";
            }
        }

        protected override string SqlInsertIdentity()
        {
            return $"INSERT INTO {Name} ({SqlField.AllFields})VALUES({SqlField.AllFieldsAt})";
        }

        #endregion


    }

    #region virtual

    internal partial class SQLiteTable<T> : ITable<T> where T : class
    {
        public override bool Update(T model, List<string> fields = null)
        {
            string updatefields;
            if (fields == null)
                updatefields = SqlField.AllFieldsAtEqExceptKey;
            else
                updatefields = CommonUtil.GetFieldsAtEqStr(fields, "", "");
            return DataBase.Execute($"UPDATE {Name} SET {updatefields} WHERE {SqlField.PrimaryKey}=@{SqlField.PrimaryKey}", model) > 0;
        }

        public override bool UpdateIgnore(T model, List<string> fields)
        {
            string updateFields = CommonUtil.GetFieldsAtEqStr(SqlField.AllFieldExceptKeyList.Except(fields), "", "");
            return DataBase.Execute($"UPDATE {Name} SET {updateFields} WHERE {SqlField.PrimaryKey}=@{SqlField.PrimaryKey}", model) > 0;
        }
    }

    #endregion

    #region abstract
    internal partial class SQLiteTable<T> : ITable<T> where T : class
    {
        public override int UpdateByWhere(T model, string where, List<string> fields = null)
        {
            string updatefields;
            if (fields != null)
            {
                updatefields = CommonUtil.GetFieldsAtEqStr(fields, "", "");
            }
            else
            {
                updatefields = SqlField.AllFieldsAtEqExceptKey;
            }
            return DataBase.Execute($"UPDATE {Name} SET {updatefields} {where}", model);
        }

        public override int UpdateByWhereIgnore(T model, string where, List<string> fields)
        {
            string updatefields = CommonUtil.GetFieldsAtEqStr(SqlField.AllFieldExceptKeyList.Except(fields), "", "");
            return DataBase.Execute($"UPDATE {Name} SET {updatefields} {where}", model);
        }

        public override int UpdateByWhere(string where, object param, List<string> fields = null)
        {
            string updatefields;
            if (fields != null)
            {
                updatefields = CommonUtil.GetFieldsAtEqStr(fields, "", "");
            }
            else
            {
                updatefields = SqlField.AllFieldsAtEqExceptKey;
            }
            return DataBase.Execute($"UPDATE {Name} SET {updatefields} {where}", param);
        }

        public override bool Delete(object id)
        {
            return DataBase.Execute($"DELETE FROM {Name} WHERE {SqlField.PrimaryKey}=@id", new { id }) > 0;
        }

        public override int DeleteByIds(object ids)
        {
            if (CommonUtil.ObjectIsEmpty(ids))
                return 0;
            var dpar = new DynamicParameters();
            dpar.Add("@ids", ids);
            return DataBase.Execute($"DELETE FROM {Name} WHERE {SqlField.PrimaryKey} IN @ids", dpar);
        }

        public override int DeleteByWhere(string where, object param)
        {
            return DataBase.Execute($"DELETE FROM {Name} {where}", param);
        }

        public override int DeleteAll()
        {
            return DataBase.Execute($"DELETE FROM {Name}");
        }

        public override bool Exists(object id)
        {
            return DataBase.ExecuteScalar($"SELECT 1 FROM {Name} WHERE {SqlField.PrimaryKey}=@id", new { id }) != null;
        }

        public override long Count(string where, object param = null)
        {
            return DataBase.ExecuteScalar<long>($"SELECT COUNT(1) FROM {Name} {where}", param);
        }

        public override TValue Min<TValue>(string field, string where = null, object param = null)
        {
            return DataBase.ExecuteScalar<TValue>($"SELECT MIN({field}) FROM {Name} {where}", param);
        }

        public override TValue Max<TValue>(string field, string where = null, object param = null)
        {
            return DataBase.ExecuteScalar<TValue>($"SELECT MAX({field}) FROM {Name} {where}", param);
        }

        public override TValue Sum<TValue>(string field, string where = null, object param = null)
        {
            return DataBase.ExecuteScalar<TValue>($"SELECT SUM({field}) FROM {Name} {where}", param);
        }

        public override decimal Avg(string field, string where = null, object param = null)
        {
            return DataBase.ExecuteScalar<decimal>($"SELECT AVG({field}) FROM {Name} {where}", param);
        }
    }
    #endregion

    #region abstract query method

    internal partial class SQLiteTable<T> : ITable<T> where T : class
    {
        public override IEnumerable<T> GetAll(string returnFields = null, string orderby = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return DataBase.Query<T>($"SELECT {returnFields} FROM {Name} {orderby.SetOrderBy(SqlField.PrimaryKey)}");
        }

        public override T GetById(object id, string returnFields = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return DataBase.QueryFirstOrDefault<T>($"SELECT {returnFields} FROM {Name} WHERE {SqlField.PrimaryKey}=@id", new { id });
        }

        public override T GetByIdForUpdate(object id, string returnFields = null)
        {
            throw new NotImplementedException();
        }

        public override IEnumerable<T> GetByIds(object ids, string returnFields = null)
        {
            if (CommonUtil.ObjectIsEmpty(ids))
                return Enumerable.Empty<T>();
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            var dpar = new DynamicParameters();
            dpar.Add("@ids", ids);
            return DataBase.Query<T>($"SELECT {returnFields} FROM {Name} WHERE {SqlField.PrimaryKey} IN @ids", dpar);
        }

        public override IEnumerable<T> GetByIdsForUpdate(object ids, string returnFields = null)
        {
            throw new NotImplementedException();
        }

        public override IEnumerable<T> GetByIdsWithField(object ids, string field, string returnFields = null)
        {
            if (CommonUtil.ObjectIsEmpty(ids))
                return Enumerable.Empty<T>();
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            var dpar = new DynamicParameters();
            dpar.Add("@ids", ids);
            return DataBase.Query<T>($"SELECT {returnFields} FROM {Name} WHERE {field} IN @ids", dpar);
        }

        public override IEnumerable<T> GetByWhere(string where, object param = null, string returnFields = null, string orderby = null, int limit = 0)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            string limitStr = null;
            if (limit != 0)
            {
                limitStr = "LIMIT " + limit;
            }
            return DataBase.Query<T>($"SELECT {returnFields} FROM {Name} {where} {orderby.SetOrderBy(SqlField.PrimaryKey)} {limitStr}", param);
        }

        public override T GetByWhereFirst(string where, object param = null, string returnFields = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return DataBase.QueryFirstOrDefault<T>($"SELECT {returnFields} FROM {Name} {where} LIMIT 1", param);
        }

        public override IEnumerable<T> GetBySkipTake(int skip, int take, string where = null, object param = null, string returnFields = null, string orderby = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return DataBase.Query<T>($"SELECT {returnFields} FROM {Name} {where} {orderby.SetOrderBy(SqlField.PrimaryKey)} LIMIT {skip},{take}", param);
        }

        public override IEnumerable<T> GetByAscFirstPage(int pageSize, object param = null, string and = null, string returnFields = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return DataBase.Query<T>($"SELECT {returnFields} FROM {Name} AS A WHERE 1=1 {and} ORDER BY {SqlField.PrimaryKey} LIMIT {pageSize}", param);
        }

        public override IEnumerable<T> GetByAscPrevPage(int pageSize, T param, string and = null, string returnFields = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return DataBase.Query<T>($"SELECT * FROM (SELECT {returnFields} FROM {Name} AS A WHERE {SqlField.PrimaryKey}<@{SqlField.PrimaryKey} {and} ORDER BY {SqlField.PrimaryKey} DESC LIMIT {pageSize}) AS B ORDER BY {SqlField.PrimaryKey}", param);
        }

        public override IEnumerable<T> GetByAscCurrentPage(int pageSize, T param, string and = null, string returnFields = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return DataBase.Query<T>($"SELECT {returnFields} FROM {Name} AS A WHERE {SqlField.PrimaryKey}>=@{SqlField.PrimaryKey} {and} ORDER BY {SqlField.PrimaryKey} LIMIT {pageSize}", param);
        }

        public override IEnumerable<T> GetByAscNextPage(int pageSize, T param, string and = null, string returnFields = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return DataBase.Query<T>($"SELECT {returnFields} FROM {Name} AS A WHERE {SqlField.PrimaryKey}>@{SqlField.PrimaryKey} {and} ORDER BY {SqlField.PrimaryKey} LIMIT {pageSize}", param);
        }

        public override IEnumerable<T> GetByAscLastPage(int pageSize, object param = null, string and = null, string returnFields = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return DataBase.Query<T>($"SELECT * FROM (SELECT {returnFields} FROM {Name} AS A WHERE 1=1 {and} ORDER BY {SqlField.PrimaryKey} DESC LIMIT {pageSize}) AS B ORDER BY {SqlField.PrimaryKey}", param);
        }

        public override IEnumerable<T> GetByDescFirstPage(int pageSize, object param = null, string and = null, string returnFields = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return DataBase.Query<T>($"SELECT {returnFields} FROM {Name} AS A WHERE 1=1 {and} ORDER BY {SqlField.PrimaryKey} DESC LIMIT {pageSize}", param);
        }

        public override IEnumerable<T> GetByDescPrevPage(int pageSize, T param, string and = null, string returnFields = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return DataBase.Query<T>($"SELECT * FROM (SELECT {returnFields} FROM {Name} AS A WHERE {SqlField.PrimaryKey}>@{SqlField.PrimaryKey} {and} ORDER BY {SqlField.PrimaryKey} LIMIT {pageSize}) AS B ORDER BY {SqlField.PrimaryKey} DESC", param);
        }

        public override IEnumerable<T> GetByDescCurrentPage(int pageSize, T param, string and = null, string returnFields = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return DataBase.Query<T>($"SELECT {returnFields} FROM {Name} AS A WHERE {SqlField.PrimaryKey}<=@{SqlField.PrimaryKey} {and} ORDER BY {SqlField.PrimaryKey} DESC LIMIT {pageSize}", param);
        }

        public override IEnumerable<T> GetByDescNextPage(int pageSize, T param, string and = null, string returnFields = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return DataBase.Query<T>($"SELECT {returnFields} FROM {Name} AS A WHERE {SqlField.PrimaryKey}<@{SqlField.PrimaryKey} {and} ORDER BY {SqlField.PrimaryKey} DESC LIMIT {pageSize}", param);
        }

        public override IEnumerable<T> GetByDescLastPage(int pageSize, object param = null, string and = null, string returnFields = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return DataBase.Query<T>($"SELECT * FROM (SELECT {returnFields} FROM {Name} AS A WHERE 1=1 {and} ORDER BY {SqlField.PrimaryKey} LIMIT {pageSize}) AS B ORDER BY {SqlField.PrimaryKey} DESC", param);
        }

    }

    #endregion

    #region abstract query method dynamic

    internal partial class SQLiteTable<T> : ITable<T> where T : class
    {

        public override IEnumerable<dynamic> GetAllDynamic(string returnFields = null, string orderby = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return DataBase.Query($"SELECT {returnFields} FROM {Name} {orderby.SetOrderBy(SqlField.PrimaryKey)}");
        }

        public override dynamic GetByIdDynamic(object id, string returnFields = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return DataBase.QueryFirstOrDefault($"SELECT {returnFields} FROM {Name} WHERE {SqlField.PrimaryKey}=@id", new { id });
        }

        public override dynamic GetByIdForUpdateDynamic(object id, string returnFields = null)
        {
            throw new NotImplementedException();
        }

        public override IEnumerable<dynamic> GetByIdsDynamic(object ids, string returnFields = null)
        {
            if (CommonUtil.ObjectIsEmpty(ids))
                return Enumerable.Empty<dynamic>();
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            var dpar = new DynamicParameters();
            dpar.Add("@ids", ids);
            return DataBase.Query($"SELECT {returnFields} FROM {Name} WHERE {SqlField.PrimaryKey} IN @ids", dpar);
        }

        public override IEnumerable<dynamic> GetByIdsForUpdateDynamic(object ids, string returnFields = null)
        {
            throw new NotImplementedException();
        }

        public override IEnumerable<dynamic> GetByIdsWithFieldDynamic(object ids, string field, string returnFields = null)
        {
            if (CommonUtil.ObjectIsEmpty(ids))
                return Enumerable.Empty<dynamic>();
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            var dpar = new DynamicParameters();
            dpar.Add("@ids", ids);
            return DataBase.Query($"SELECT {returnFields} FROM {Name} WHERE {field} IN @ids", dpar);
        }

        public override IEnumerable<dynamic> GetByWhereDynamic(string where, object param = null, string returnFields = null, string orderby = null, int limit = 0)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            string limitStr = null;
            if (limit != 0)
            {
                limitStr = "LIMIT " + limit;
            }
            return DataBase.Query($"SELECT {returnFields} FROM {Name} {where} {orderby.SetOrderBy(SqlField.PrimaryKey)} {limitStr}", param);
        }

        public override dynamic GetByWhereFirstDynamic(string where, object param = null, string returnFields = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return DataBase.QueryFirstOrDefault($"SELECT {returnFields} FROM {Name} {where} LIMIT 1", param);
        }

        public override IEnumerable<dynamic> GetBySkipTakeDynamic(int skip, int take, string where = null, object param = null, string returnFields = null, string orderby = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return DataBase.Query($"SELECT {returnFields} FROM {Name} {where} {orderby.SetOrderBy(SqlField.PrimaryKey)} LIMIT {skip},{take}", param);
        }

        public override IEnumerable<dynamic> GetByAscFirstPageDynamic(int pageSize, object param = null, string and = null, string returnFields = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return DataBase.Query($"SELECT {returnFields} FROM {Name} AS A WHERE 1=1 {and} ORDER BY {SqlField.PrimaryKey} LIMIT {pageSize}", param);
        }

        public override IEnumerable<dynamic> GetByAscPrevPageDynamic(int pageSize, T param, string and = null, string returnFields = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return DataBase.Query($"SELECT * FROM (SELECT {returnFields} FROM {Name} AS A WHERE {SqlField.PrimaryKey}<@{SqlField.PrimaryKey} {and} ORDER BY {SqlField.PrimaryKey} DESC LIMIT {pageSize}) AS B ORDER BY {SqlField.PrimaryKey}", param);
        }

        public override IEnumerable<dynamic> GetByAscCurrentPageDynamic(int pageSize, T param, string and = null, string returnFields = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return DataBase.Query($"SELECT {returnFields} FROM {Name} AS A WHERE {SqlField.PrimaryKey}>=@{SqlField.PrimaryKey} {and} ORDER BY {SqlField.PrimaryKey} LIMIT {pageSize}", param);
        }

        public override IEnumerable<dynamic> GetByAscNextPageDynamic(int pageSize, T param, string and = null, string returnFields = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return DataBase.Query($"SELECT {returnFields} FROM {Name} AS A WHERE {SqlField.PrimaryKey}>@{SqlField.PrimaryKey} {and} ORDER BY {SqlField.PrimaryKey} LIMIT {pageSize}", param);
        }

        public override IEnumerable<dynamic> GetByAscLastPageDynamic(int pageSize, object param = null, string and = null, string returnFields = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return DataBase.Query($"SELECT * FROM (SELECT {returnFields} FROM {Name} AS A WHERE 1=1 {and} ORDER BY {SqlField.PrimaryKey} DESC LIMIT {pageSize}) AS B ORDER BY {SqlField.PrimaryKey}", param);
        }

        public override IEnumerable<dynamic> GetByDescFirstPageDynamic(int pageSize, object param = null, string and = null, string returnFields = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return DataBase.Query($"SELECT {returnFields} FROM {Name} AS A WHERE 1=1 {and} ORDER BY {SqlField.PrimaryKey} DESC LIMIT {pageSize}", param);
        }

        public override IEnumerable<dynamic> GetByDescPrevPageDynamic(int pageSize, T param, string and = null, string returnFields = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return DataBase.Query($"SELECT * FROM (SELECT {returnFields} FROM {Name} AS A WHERE {SqlField.PrimaryKey}>@{SqlField.PrimaryKey} {and} ORDER BY {SqlField.PrimaryKey} LIMIT {pageSize}) AS B ORDER BY {SqlField.PrimaryKey} DESC", param);
        }

        public override IEnumerable<dynamic> GetByDescCurrentPageDynamic(int pageSize, T param, string and = null, string returnFields = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return DataBase.Query($"SELECT {returnFields} FROM {Name} AS A WHERE {SqlField.PrimaryKey}<=@{SqlField.PrimaryKey} {and} ORDER BY {SqlField.PrimaryKey} DESC LIMIT {pageSize}", param);
        }

        public override IEnumerable<dynamic> GetByDescNextPageDynamic(int pageSize, T param, string and = null, string returnFields = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return DataBase.Query($"SELECT {returnFields} FROM {Name} AS A WHERE {SqlField.PrimaryKey}<@{SqlField.PrimaryKey} {and} ORDER BY {SqlField.PrimaryKey} DESC LIMIT {pageSize}", param);
        }

        public override IEnumerable<dynamic> GetByDescLastPageDynamic(int pageSize, object param = null, string and = null, string returnFields = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return DataBase.Query($"SELECT * FROM (SELECT {returnFields} FROM {Name} AS A WHERE 1=1 {and} ORDER BY {SqlField.PrimaryKey} LIMIT {pageSize}) AS B ORDER BY {SqlField.PrimaryKey} DESC", param);
        }
    }

    #endregion
}
