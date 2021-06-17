using FastMember;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace Dapper.Sharding
{
    internal partial class PostgreTable<T> : ITable<T> where T : class
    {
        public PostgreTable(string name, IDatabase database) : base(name, database, SqlFieldCacheUtils.GetPostgreFieldEntity<T>())
        {

        }

        #region insert

        protected override string SqlInsert()
        {
            if (SqlField.IsIdentity)
            {
                return $"INSERT INTO {Name} ({SqlField.AllFieldsExceptKey})VALUES({SqlField.AllFieldsAtExceptKey}) RETURNING {SqlField.PrimaryKey}";
            }
            else
            {
                return $"INSERT INTO {Name} ({SqlField.AllFields})VALUES({SqlField.AllFieldsAt})";
            }
        }

        protected override string SqlInsertIdentity()
        {
            return $"INSERT INTO {Name} ({SqlField.AllFields})VALUES({SqlField.AllFieldsAt})";
        }



        #endregion

        #region update

        protected override string SqlUpdate(List<string> fields = null)
        {
            string updatefields;
            if (fields == null)
                updatefields = SqlField.AllFieldsAtEqExceptKey;
            else
                updatefields = CommonUtil.GetFieldsAtEqStr(fields, "", "", entity: SqlField);
            return $"UPDATE {Name} SET {updatefields} WHERE {SqlField.PrimaryKey}=@{SqlField.PrimaryKey}";
        }

        protected override string SqlUpdateIgnore(List<string> fields)
        {
            string updateFields = CommonUtil.GetFieldsAtEqStr(SqlField.AllFieldExceptKeyList.Except(fields), "", "", entity: SqlField);
            return $"UPDATE {Name} SET {updateFields} WHERE {SqlField.PrimaryKey}=@{SqlField.PrimaryKey}";
        }

        protected override string SqlUpdateByWhere(string where, List<string> fields = null)
        {
            string updatefields;
            if (fields != null)
            {
                updatefields = CommonUtil.GetFieldsAtEqStr(fields, "", "", entity: SqlField);
            }
            else
            {
                updatefields = SqlField.AllFieldsAtEqExceptKey;
            }
            return $"UPDATE {Name} SET {updatefields} {where}";
        }

        protected override string SqlUpdateByWhereIgnore(string where, List<string> fields)
        {
            string updatefields = CommonUtil.GetFieldsAtEqStr(SqlField.AllFieldExceptKeyList.Except(fields), "", "", entity: SqlField);
            return $"UPDATE {Name} SET {updatefields} {where}";
        }

        #endregion

        #region delete

        protected override string SqlDeleteById()
        {
            return $"DELETE FROM {Name} WHERE {SqlField.PrimaryKey}=@id";
        }

        protected override string SqlDeleteByIds()
        {
            return $"DELETE FROM {Name} WHERE {SqlField.PrimaryKey}=ANY(@ids)";
        }

        protected override string SqlDeleteByWhere(string where)
        {
            return $"DELETE FROM {Name} {where}";
        }

        protected override string SqlDeleteAll()
        {
            return $"DELETE FROM {Name}";
        }

        #endregion
    }


    #region abstract
    internal partial class PostgreTable<T> : ITable<T> where T : class
    {

        public override bool Exists(object id)
        {
            return DataBase.ExecuteScalar($"SELECT 1 FROM {Name} WHERE {SqlField.PrimaryKey}=@id", new { id }) != null;
        }

        public override long Count(string where = null, object param = null)
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

    internal partial class PostgreTable<T> : ITable<T> where T : class
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
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return DataBase.QueryFirstOrDefault<T>($"SELECT {returnFields} FROM {Name} WHERE {SqlField.PrimaryKey}=@id FOR UPDATE", new { id });
        }

        public override IEnumerable<T> GetByIds(object ids, string returnFields = null)
        {
            if (CommonUtil.ObjectIsEmpty(ids))
                return Enumerable.Empty<T>();
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            var dpar = new DynamicParameters();
            dpar.Add("@ids", ids);
            return DataBase.Query<T>($"SELECT {returnFields} FROM {Name} WHERE {SqlField.PrimaryKey}=ANY(@ids)", dpar);
        }

        public override IEnumerable<T> GetByIdsForUpdate(object ids, string returnFields = null)
        {
            if (CommonUtil.ObjectIsEmpty(ids))
                return Enumerable.Empty<T>();
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            var dpar = new DynamicParameters();
            dpar.Add("@ids", ids);
            return DataBase.Query<T>($"SELECT {returnFields} FROM {Name} WHERE {SqlField.PrimaryKey}=ANY(@ids) FOR UPDATE", dpar);
        }

        public override IEnumerable<T> GetByIdsWithField(object ids, string field, string returnFields = null)
        {
            if (CommonUtil.ObjectIsEmpty(ids))
                return Enumerable.Empty<T>();
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            var dpar = new DynamicParameters();
            dpar.Add("@ids", ids);
            return DataBase.Query<T>($"SELECT {returnFields} FROM {Name} WHERE {field}=ANY(@ids)", dpar);
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
            return DataBase.Query<T>($"SELECT {returnFields} FROM {Name} {where} {orderby.SetOrderBy(SqlField.PrimaryKey)} LIMIT {take} OFFSET {skip}", param);
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

    internal partial class PostgreTable<T> : ITable<T> where T : class
    {
        public override IEnumerable<dynamic> GetAllDynamic(string returnFields = null, string orderby = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            if (!returnFields.Contains(" AS "))
            {
                returnFields = returnFields.AsPgsqlField();
            }
            return DataBase.Query($"SELECT {returnFields} FROM {Name} {orderby.SetOrderBy(SqlField.PrimaryKey)}");
        }

        public override dynamic GetByIdDynamic(object id, string returnFields = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            if (!returnFields.Contains(" AS "))
            {
                returnFields = returnFields.AsPgsqlField();
            }
            return DataBase.QueryFirstOrDefault($"SELECT {returnFields} FROM {Name} WHERE {SqlField.PrimaryKey}=@id", new { id });
        }

        public override dynamic GetByIdForUpdateDynamic(object id, string returnFields = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            if (!returnFields.Contains(" AS "))
            {
                returnFields = returnFields.AsPgsqlField();
            }
            return DataBase.QueryFirstOrDefault($"SELECT {returnFields} FROM {Name} WHERE {SqlField.PrimaryKey}=@id FOR UPDATE", new { id });
        }

        public override IEnumerable<dynamic> GetByIdsDynamic(object ids, string returnFields = null)
        {
            if (CommonUtil.ObjectIsEmpty(ids))
                return Enumerable.Empty<dynamic>();
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            if (!returnFields.Contains(" AS "))
            {
                returnFields = returnFields.AsPgsqlField();
            }
            var dpar = new DynamicParameters();
            dpar.Add("@ids", ids);
            return DataBase.Query($"SELECT {returnFields} FROM {Name} WHERE {SqlField.PrimaryKey}=ANY(@ids)", dpar);
        }

        public override IEnumerable<dynamic> GetByIdsForUpdateDynamic(object ids, string returnFields = null)
        {
            if (CommonUtil.ObjectIsEmpty(ids))
                return Enumerable.Empty<dynamic>();
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            if (!returnFields.Contains(" AS "))
            {
                returnFields = returnFields.AsPgsqlField();
            }
            var dpar = new DynamicParameters();
            dpar.Add("@ids", ids);
            return DataBase.Query($"SELECT {returnFields} FROM {Name} WHERE {SqlField.PrimaryKey}=ANY(@ids) FOR UPDATE", dpar);
        }

        public override IEnumerable<dynamic> GetByIdsWithFieldDynamic(object ids, string field, string returnFields = null)
        {
            if (CommonUtil.ObjectIsEmpty(ids))
                return Enumerable.Empty<dynamic>();
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            if (!returnFields.Contains(" AS "))
            {
                returnFields = returnFields.AsPgsqlField();
            }
            var dpar = new DynamicParameters();
            dpar.Add("@ids", ids);
            return DataBase.Query($"SELECT {returnFields} FROM {Name} WHERE {field}=ANY(@ids)", dpar);
        }

        public override IEnumerable<dynamic> GetByWhereDynamic(string where, object param = null, string returnFields = null, string orderby = null, int limit = 0)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            if (!returnFields.Contains(" AS "))
            {
                returnFields = returnFields.AsPgsqlField();
            }
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
            if (!returnFields.Contains(" AS "))
            {
                returnFields = returnFields.AsPgsqlField();
            }
            return DataBase.QueryFirstOrDefault($"SELECT {returnFields} FROM {Name} {where} LIMIT 1", param);
        }

        public override IEnumerable<dynamic> GetBySkipTakeDynamic(int skip, int take, string where = null, object param = null, string returnFields = null, string orderby = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            if (!returnFields.Contains(" AS "))
            {
                returnFields = returnFields.AsPgsqlField();
            }
            return DataBase.Query($"SELECT {returnFields} FROM {Name} {where} {orderby.SetOrderBy(SqlField.PrimaryKey)} LIMIT {take} OFFSET {skip}", param);
        }

        public override IEnumerable<dynamic> GetByAscFirstPageDynamic(int pageSize, object param = null, string and = null, string returnFields = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            if (!returnFields.Contains(" AS "))
            {
                returnFields = returnFields.AsPgsqlField();
            }
            return DataBase.Query($"SELECT {returnFields} FROM {Name} AS A WHERE 1=1 {and} ORDER BY {SqlField.PrimaryKey} LIMIT {pageSize}", param);
        }

        public override IEnumerable<dynamic> GetByAscPrevPageDynamic(int pageSize, T param, string and = null, string returnFields = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            if (!returnFields.Contains(" AS "))
            {
                returnFields = returnFields.AsPgsqlField();
            }
            return DataBase.Query($"SELECT * FROM (SELECT {returnFields} FROM {Name} AS A WHERE {SqlField.PrimaryKey}<@{SqlField.PrimaryKey} {and} ORDER BY {SqlField.PrimaryKey} DESC LIMIT {pageSize}) AS B ORDER BY {SqlField.PrimaryKey}", param);
        }

        public override IEnumerable<dynamic> GetByAscCurrentPageDynamic(int pageSize, T param, string and = null, string returnFields = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            if (!returnFields.Contains(" AS "))
            {
                returnFields = returnFields.AsPgsqlField();
            }
            return DataBase.Query($"SELECT {returnFields} FROM {Name} AS A WHERE {SqlField.PrimaryKey}>=@{SqlField.PrimaryKey} {and} ORDER BY {SqlField.PrimaryKey} LIMIT {pageSize}", param);
        }

        public override IEnumerable<dynamic> GetByAscNextPageDynamic(int pageSize, T param, string and = null, string returnFields = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            if (!returnFields.Contains(" AS "))
            {
                returnFields = returnFields.AsPgsqlField();
            }
            return DataBase.Query($"SELECT {returnFields} FROM {Name} AS A WHERE {SqlField.PrimaryKey}>@{SqlField.PrimaryKey} {and} ORDER BY {SqlField.PrimaryKey} LIMIT {pageSize}", param);
        }

        public override IEnumerable<dynamic> GetByAscLastPageDynamic(int pageSize, object param = null, string and = null, string returnFields = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            if (!returnFields.Contains(" AS "))
            {
                returnFields = returnFields.AsPgsqlField();
            }
            return DataBase.Query($"SELECT * FROM (SELECT {returnFields} FROM {Name} AS A WHERE 1=1 {and} ORDER BY {SqlField.PrimaryKey} DESC LIMIT {pageSize}) AS B ORDER BY {SqlField.PrimaryKey}", param);
        }

        public override IEnumerable<dynamic> GetByDescFirstPageDynamic(int pageSize, object param = null, string and = null, string returnFields = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            if (!returnFields.Contains(" AS "))
            {
                returnFields = returnFields.AsPgsqlField();
            }
            return DataBase.Query($"SELECT {returnFields} FROM {Name} AS A WHERE 1=1 {and} ORDER BY {SqlField.PrimaryKey} DESC LIMIT {pageSize}", param);
        }

        public override IEnumerable<dynamic> GetByDescPrevPageDynamic(int pageSize, T param, string and = null, string returnFields = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            if (!returnFields.Contains(" AS "))
            {
                returnFields = returnFields.AsPgsqlField();
            }
            return DataBase.Query($"SELECT * FROM (SELECT {returnFields} FROM {Name} AS A WHERE {SqlField.PrimaryKey}>@{SqlField.PrimaryKey} {and} ORDER BY {SqlField.PrimaryKey} LIMIT {pageSize}) AS B ORDER BY {SqlField.PrimaryKey} DESC", param);
        }

        public override IEnumerable<dynamic> GetByDescCurrentPageDynamic(int pageSize, T param, string and = null, string returnFields = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            if (!returnFields.Contains(" AS "))
            {
                returnFields = returnFields.AsPgsqlField();
            }
            return DataBase.Query($"SELECT {returnFields} FROM {Name} AS A WHERE {SqlField.PrimaryKey}<=@{SqlField.PrimaryKey} {and} ORDER BY {SqlField.PrimaryKey} DESC LIMIT {pageSize}", param);
        }

        public override IEnumerable<dynamic> GetByDescNextPageDynamic(int pageSize, T param, string and = null, string returnFields = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            if (!returnFields.Contains(" AS "))
            {
                returnFields = returnFields.AsPgsqlField();
            }
            return DataBase.Query($"SELECT {returnFields} FROM {Name} AS A WHERE {SqlField.PrimaryKey}<@{SqlField.PrimaryKey} {and} ORDER BY {SqlField.PrimaryKey} DESC LIMIT {pageSize}", param);
        }

        public override IEnumerable<dynamic> GetByDescLastPageDynamic(int pageSize, object param = null, string and = null, string returnFields = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            if (!returnFields.Contains(" AS "))
            {
                returnFields = returnFields.AsPgsqlField();
            }
            return DataBase.Query($"SELECT * FROM (SELECT {returnFields} FROM {Name} AS A WHERE 1=1 {and} ORDER BY {SqlField.PrimaryKey} LIMIT {pageSize}) AS B ORDER BY {SqlField.PrimaryKey} DESC", param);
        }
    }

    #endregion

}
