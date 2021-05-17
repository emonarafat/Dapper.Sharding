using FastMember;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Dynamic.Core;

namespace Dapper.Sharding
{
    internal class ClickHouseTable<T> : ITable<T> where T : class
    {
        public ClickHouseTable(string name, IDatabase database, IDbConnection conn = null, IDbTransaction tran = null, int? commandTimeout = null) : base(name, database, SqlFieldCacheUtils.GetClickHouseFieldEntity<T>(), new DapperEntity(name, database, conn, tran, commandTimeout))
        {

        }

        #region method

        private IEnumerable<T> GetInsertList(IEnumerable<T> modelList)
        {
            if (modelList.Count() == 0)
            {
                return Enumerable.Empty<T>();
            }
            var query = modelList.AsQueryable();
            var ids = query.Where($"{SqlField.PrimaryKey}!=null").Select(SqlField.PrimaryKey).ToDynamicList();
            if (ids.Count == 0)
            {
                return Enumerable.Empty<T>();
            }

            var sql = $"SELECT {SqlField.PrimaryKey} FROM {Name} WHERE {SqlField.PrimaryKey} IN (@ids)";
            var dpar = new DynamicParameters();
            dpar.Add("@ids", ids);
            var data = DpEntity.Query(sql, dpar);
            var existsIds = data.Select(s => ((IDictionary<string, object>)s).First().Value);

            var insertIds = ids.Except(existsIds);
            if (insertIds.Count() == 0)
            {
                return Enumerable.Empty<T>();
            }
            return query.Where($"@0.Contains({SqlField.PrimaryKey})", insertIds).ToList();
        }

        #endregion

        #region  insert

        public override bool Insert(T model)
        {
            var sql = $"INSERT INTO {Name} ({SqlField.AllFields}) SELECT {SqlField.AllFieldsAt}";
            return DpEntity.Execute(sql, model) > 0;
        }

        public override bool Insert(T model, List<string> fields)
        {
            var ff = CommonUtil.GetFieldsStr(fields, "", "");
            var vv = CommonUtil.GetFieldsAtStr(fields, "@");
            var sql = $"INSERT INTO {Name} ({ff}) SELECT {vv}";
            return DpEntity.Execute(sql, model) > 0;
        }

        public override bool InsertIgnore(T model, List<string> fields)
        {
            var exList = SqlField.AllFieldList.Except(fields).ToList();
            var ff = CommonUtil.GetFieldsStr(exList, "", "");
            var vv = CommonUtil.GetFieldsAtStr(exList, "@");
            var sql = $"INSERT INTO {Name} ({ff}) SELECT {vv}";
            return DpEntity.Execute(sql, model) > 0;

        }

        public override void Insert(IEnumerable<T> modelList)
        {
            var sql = $"INSERT INTO {Name} ({SqlField.AllFields}) VALUES @list";
            DpEntity.Execute(sql, new { list = modelList });
        }

        public override bool Insert(IEnumerable<T> modelList, List<string> fields)
        {
            var ff = CommonUtil.GetFieldsStr(fields, "", "");
            var sql = $"INSERT INTO {Name} ({ff}) VALUES @list";
            return DpEntity.Execute(sql, new { list = modelList }) > 0;
        }

        public override bool InsertIgnore(IEnumerable<T> modelList, List<string> fields)
        {
            var exList = SqlField.AllFieldList.Except(fields).ToList();
            var ff = CommonUtil.GetFieldsStr(exList, "", "");
            var sql = $"INSERT INTO {Name} ({ff}) VALUES @list";
            return DpEntity.Execute(sql, new { list = modelList }) > 0;
        }

        #endregion

        #region insert if not exist

        public override void InsertIfNoExists(T model)
        {
            if (!Exists(model))
            {
                Insert(model);
            }
        }

        public override void InsertIfNoExists(T model, List<string> fields)
        {
            if (!Exists(model))
            {
                Insert(model, fields);
            }
        }

        public override void InsertIfNoExistsIgnore(T model, List<string> fields)
        {
            if (!Exists(model))
            {
                InsertIgnore(model, fields);
            }
        }

        public override void InsertIfNoExists(IEnumerable<T> modelList)
        {
            var list = GetInsertList(modelList);
            if (list.Count() > 0)
            {
                Insert(list);
            }
        }

        public override void InsertIfNoExists(IEnumerable<T> modelList, List<string> fields)
        {
            var list = GetInsertList(modelList);
            if (list.Count() > 0)
            {
                Insert(list, fields);
            }
        }

        public override void InsertIfNoExistsIgnore(IEnumerable<T> modelList, List<string> fields)
        {
            var list = GetInsertList(modelList);
            if (list.Count() > 0)
            {
                InsertIgnore(list, fields);
            }
        }

        #endregion

        #region update

        public override bool Update(T model, List<string> fields = null)
        {
            string updatefields;
            if (fields == null)
                updatefields = SqlField.AllFieldsAtEqExceptKey;
            else
                updatefields = CommonUtil.GetFieldsAtEqStr(fields, "", "");
            return DpEntity.Execute($"ALTER TABLE {Name} UPDATE {updatefields} WHERE {SqlField.PrimaryKey}=@{SqlField.PrimaryKey}", model) > 0;
        }

        public override void Update(IEnumerable<T> modelList, List<string> fields = null)
        {
            foreach (var item in modelList)
            {
                Update(item, fields);
            }
        }

        public override bool UpdateIgnore(T model, List<string> fields)
        {
            string updateFields = CommonUtil.GetFieldsAtEqStr(SqlField.AllFieldExceptKeyList.Except(fields), "", "");
            return DpEntity.Execute($"ALTER TABLE {Name} UPDATE {updateFields} WHERE {SqlField.PrimaryKey}=@{SqlField.PrimaryKey}", model) > 0;
        }

        public override void UpdateIgnore(IEnumerable<T> modelList, List<string> fields)
        {
            foreach (var item in modelList)
            {
                UpdateIgnore(item, fields);
            }
        }

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
            return DpEntity.Execute($"ALTER TABLE {Name} UPDATE {updatefields} {where}", model);
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
            return DpEntity.Execute($"ALTER TABLE {Name} UPDATE {updatefields} {where}", param);
        }

        public override int UpdateByWhereIgnore(T model, string where, List<string> fields)
        {
            string updateFields = CommonUtil.GetFieldsAtEqStr(SqlField.AllFieldExceptKeyList.Except(fields), "", "");
            return DpEntity.Execute($"ALTER TABLE {Name} UPDATE {updateFields} {where}", model);

        }

        #endregion

        #region del

        public override bool Delete(object id)
        {
            var sql = $"ALTER TABLE {Name} DELETE WHERE {SqlField.PrimaryKey}=@id";
            return DpEntity.Execute(sql, new { id }) > 0;
        }

        public override int DeleteAll()
        {
            DataBase.DropTable(Name);
            DataBase.CreateTable<T>(Name);
            return 1;
        }

        public override int DeleteByIds(object ids)
        {
            if (CommonUtil.ObjectIsEmpty(ids))
                return 0;
            var dpar = new DynamicParameters();
            dpar.Add("@ids", ids);
            var sql = $"ALTER TABLE {Name} DELETE WHERE {SqlField.PrimaryKey} IN (@ids)";
            return DpEntity.Execute(sql, dpar);
        }

        public override int DeleteByWhere(string where, object param = null)
        {
            var sql = $"ALTER TABLE {Name} DELETE {where}";
            return DpEntity.Execute(sql, param);
        }

        public override void Delete(T model)
        {
            var accessor = TypeAccessor.Create(typeof(T));
            var id = accessor[model, SqlField.PrimaryKey];
            Delete(id);
        }

        public override void Delete(IEnumerable<T> modelList)
        {
            if (modelList.Count() == 0)
            {
                return;
            }
            var query = modelList.AsQueryable();
            var ids = query.Where($"{SqlField.PrimaryKey}!=null").Select(SqlField.PrimaryKey).ToDynamicList();
            if (ids.Count == 0)
            {
                return;
            }
            var dpar = new DynamicParameters();
            dpar.Add("@ids", ids);
            var sql = $"ALTER TABLE {Name} DELETE WHERE {SqlField.PrimaryKey} IN (@ids)";
            DpEntity.Execute(sql, dpar);
        }


        #endregion

        #region insert identity merge

        public override bool InsertIdentity(T model)
        {
            throw new NotImplementedException();
        }

        public override bool InsertIdentity(T model, List<string> fields)
        {
            throw new NotImplementedException();
        }

        public override bool InsertIdentityIgnore(T model, List<string> fields)
        {
            throw new NotImplementedException();
        }

        public override void InsertIdentity(IEnumerable<T> modelList)
        {
            throw new NotImplementedException();
        }

        public override void InsertIdentity(IEnumerable<T> modelList, List<string> fields)
        {
            throw new NotImplementedException();
        }

        public override void InsertIdentityIgnore(IEnumerable<T> modelList, List<string> fields)
        {
            throw new NotImplementedException();
        }

        public override void InsertIdentityIfNoExists(T model)
        {
            throw new NotImplementedException();
        }

        public override void InsertIdentityIfNoExists(T model, List<string> fields)
        {
            throw new NotImplementedException();
        }

        public override void InsertIdentityIfNoExistsIgnore(T model, List<string> fields)
        {
            throw new NotImplementedException();
        }

        public override void InsertIdentityIfNoExists(IEnumerable<T> modelList)
        {
            throw new NotImplementedException();
        }

        public override void InsertIdentityIfNoExists(IEnumerable<T> modelList, List<string> fields)
        {
            throw new NotImplementedException();
        }

        public override void InsertIdentityIfNoExistsIgnore(IEnumerable<T> modelList, List<string> fields)
        {
            throw new NotImplementedException();
        }

        public override void Merge(T model, List<string> fields = null)
        {
            throw new NotImplementedException();
        }

        public override void Merge(IEnumerable<T> modelList, List<string> fields = null)
        {
            throw new NotImplementedException();
        }

        public override void MergeIgnore(T model, List<string> fields = null)
        {
            throw new NotImplementedException();
        }

        public override void MergeIgnore(IEnumerable<T> modelList, List<string> fields = null)
        {
            throw new NotImplementedException();
        }

        #endregion

        #region abstract pub

        public override ITable<T> CreateTranTable(IDbConnection conn, IDbTransaction tran, int? commandTimeout = null)
        {
            throw new NotImplementedException();
        }

        public override long Count(string where = null, object param = null)
        {
            if (!string.IsNullOrEmpty(SqlField.PrimaryKey))
            {
                return DpEntity.ExecuteScalar<long>($"SELECT COUNT({SqlField.PrimaryKey}) FROM {Name} {where}", param);
            }
            return DpEntity.ExecuteScalar<long>($"SELECT COUNT() FROM {Name} {where}", param);
        }

        public override bool Exists(object id)
        {
            return DpEntity.ExecuteScalar<long>($"SELECT COUNT({SqlField.PrimaryKey}) FROM {Name} WHERE {SqlField.PrimaryKey}=@id", new { id }) > 0;
        }

        public override TValue Min<TValue>(string field, string where = null, object param = null)
        {
            return DpEntity.ExecuteScalar<TValue>($"SELECT MIN({field}) FROM {Name} {where}", param);
        }

        public override TValue Max<TValue>(string field, string where = null, object param = null)
        {
            return DpEntity.ExecuteScalar<TValue>($"SELECT MAX({field}) FROM {Name} {where}", param);
        }

        public override TValue Sum<TValue>(string field, string where = null, object param = null)
        {
            return DpEntity.ExecuteScalar<TValue>($"SELECT SUM({field}) FROM {Name} {where}", param);
        }

        public override decimal Avg(string field, string where = null, object param = null)
        {
            return DpEntity.ExecuteScalar<decimal>($"SELECT AVG({field}) FROM {Name} {where}", param);
        }

        #endregion

        #region dynamic no

        public override IEnumerable<T> GetAll(string returnFields = null, string orderby = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return DpEntity.Query<T>($"SELECT {returnFields} FROM {Name} {orderby.SetOrderBy(SqlField.PrimaryKey)}");
        }

        public override T GetById(object id, string returnFields = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return DpEntity.QueryFirstOrDefault<T>($"SELECT {returnFields} FROM {Name} WHERE {SqlField.PrimaryKey}=@id", new { id });
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
            return DpEntity.Query<T>($"SELECT {returnFields} FROM {Name} WHERE {SqlField.PrimaryKey} IN (@ids)", dpar);
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
            return DpEntity.Query<T>($"SELECT {returnFields} FROM {Name} WHERE {field} IN (@ids)", dpar);
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
            return DpEntity.Query<T>($"SELECT {returnFields} FROM {Name} {where} {orderby.SetOrderBy(SqlField.PrimaryKey)} {limitStr}", param);
        }

        public override T GetByWhereFirst(string where, object param = null, string returnFields = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return DpEntity.QueryFirstOrDefault<T>($"SELECT {returnFields} FROM {Name} {where} LIMIT 1", param);
        }

        public override IEnumerable<T> GetBySkipTake(int skip, int take, string where = null, object param = null, string returnFields = null, string orderby = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return DpEntity.Query<T>($"SELECT {returnFields} FROM {Name} {where} {orderby.SetOrderBy(SqlField.PrimaryKey)} LIMIT {skip},{take}", param);
        }

        public override IEnumerable<T> GetByAscFirstPage(int pageSize, object param = null, string and = null, string returnFields = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return DpEntity.Query<T>($"SELECT {returnFields} FROM {Name} AS A WHERE 1=1 {and} ORDER BY {SqlField.PrimaryKey} LIMIT {pageSize}", param);
        }

        public override IEnumerable<T> GetByAscPrevPage(int pageSize, T param, string and = null, string returnFields = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return DpEntity.Query<T>($"SELECT * FROM (SELECT {returnFields} FROM {Name} AS A WHERE {SqlField.PrimaryKey}<@{SqlField.PrimaryKey} {and} ORDER BY {SqlField.PrimaryKey} DESC LIMIT {pageSize}) AS B ORDER BY {SqlField.PrimaryKey}", param);
        }

        public override IEnumerable<T> GetByAscCurrentPage(int pageSize, T param, string and = null, string returnFields = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return DpEntity.Query<T>($"SELECT {returnFields} FROM {Name} AS A WHERE {SqlField.PrimaryKey}>=@{SqlField.PrimaryKey} {and} ORDER BY {SqlField.PrimaryKey} LIMIT {pageSize}", param);
        }

        public override IEnumerable<T> GetByAscNextPage(int pageSize, T param, string and = null, string returnFields = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return DpEntity.Query<T>($"SELECT {returnFields} FROM {Name} AS A WHERE {SqlField.PrimaryKey}>@{SqlField.PrimaryKey} {and} ORDER BY {SqlField.PrimaryKey} LIMIT {pageSize}", param);
        }

        public override IEnumerable<T> GetByAscLastPage(int pageSize, object param = null, string and = null, string returnFields = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return DpEntity.Query<T>($"SELECT * FROM (SELECT {returnFields} FROM {Name} AS A WHERE 1=1 {and} ORDER BY {SqlField.PrimaryKey} DESC LIMIT {pageSize}) AS B ORDER BY {SqlField.PrimaryKey}", param);
        }

        public override IEnumerable<T> GetByDescFirstPage(int pageSize, object param = null, string and = null, string returnFields = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return DpEntity.Query<T>($"SELECT {returnFields} FROM {Name} AS A WHERE 1=1 {and} ORDER BY {SqlField.PrimaryKey} DESC LIMIT {pageSize}", param);
        }

        public override IEnumerable<T> GetByDescPrevPage(int pageSize, T param, string and = null, string returnFields = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return DpEntity.Query<T>($"SELECT * FROM (SELECT {returnFields} FROM {Name} AS A WHERE {SqlField.PrimaryKey}>@{SqlField.PrimaryKey} {and} ORDER BY {SqlField.PrimaryKey} LIMIT {pageSize}) AS B ORDER BY {SqlField.PrimaryKey} DESC", param);
        }

        public override IEnumerable<T> GetByDescCurrentPage(int pageSize, T param, string and = null, string returnFields = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return DpEntity.Query<T>($"SELECT {returnFields} FROM {Name} AS A WHERE {SqlField.PrimaryKey}<=@{SqlField.PrimaryKey} {and} ORDER BY {SqlField.PrimaryKey} DESC LIMIT {pageSize}", param);
        }

        public override IEnumerable<T> GetByDescNextPage(int pageSize, T param, string and = null, string returnFields = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return DpEntity.Query<T>($"SELECT {returnFields} FROM {Name} AS A WHERE {SqlField.PrimaryKey}<@{SqlField.PrimaryKey} {and} ORDER BY {SqlField.PrimaryKey} DESC LIMIT {pageSize}", param);
        }

        public override IEnumerable<T> GetByDescLastPage(int pageSize, object param = null, string and = null, string returnFields = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return DpEntity.Query<T>($"SELECT * FROM (SELECT {returnFields} FROM {Name} AS A WHERE 1=1 {and} ORDER BY {SqlField.PrimaryKey} LIMIT {pageSize}) AS B ORDER BY {SqlField.PrimaryKey} DESC", param);
        }

        #endregion

        #region dynamic

        public override IEnumerable<dynamic> GetAllDynamic(string returnFields = null, string orderby = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return DpEntity.Query($"SELECT {returnFields} FROM {Name} {orderby.SetOrderBy(SqlField.PrimaryKey)}");
        }

        public override dynamic GetByIdDynamic(object id, string returnFields = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return DpEntity.QueryFirstOrDefault($"SELECT {returnFields} FROM {Name} WHERE {SqlField.PrimaryKey}=@id", new { id });
        }

        public override dynamic GetByIdForUpdateDynamic(object id, string returnFields = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return DpEntity.QueryFirstOrDefault($"SELECT {returnFields} FROM {Name} WHERE {SqlField.PrimaryKey}=@id FOR UPDATE", new { id });
        }

        public override IEnumerable<dynamic> GetByIdsDynamic(object ids, string returnFields = null)
        {
            if (CommonUtil.ObjectIsEmpty(ids))
                return Enumerable.Empty<dynamic>();
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            var dpar = new DynamicParameters();
            dpar.Add("@ids", ids);
            return DpEntity.Query($"SELECT {returnFields} FROM {Name} WHERE {SqlField.PrimaryKey} IN (@ids)", dpar);
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
            return DpEntity.Query($"SELECT {returnFields} FROM {Name} WHERE {field} IN (@ids)", dpar);
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
            return DpEntity.Query($"SELECT {returnFields} FROM {Name} {where} {orderby.SetOrderBy(SqlField.PrimaryKey)} {limitStr}", param);
        }

        public override dynamic GetByWhereFirstDynamic(string where, object param = null, string returnFields = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return DpEntity.QueryFirstOrDefault($"SELECT {returnFields} FROM {Name} {where} LIMIT 1", param);
        }

        public override IEnumerable<dynamic> GetBySkipTakeDynamic(int skip, int take, string where = null, object param = null, string returnFields = null, string orderby = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return DpEntity.Query($"SELECT {returnFields} FROM {Name} {where} {orderby.SetOrderBy(SqlField.PrimaryKey)} LIMIT {skip},{take}", param);
        }

        public override IEnumerable<dynamic> GetByAscFirstPageDynamic(int pageSize, object param = null, string and = null, string returnFields = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return DpEntity.Query($"SELECT {returnFields} FROM {Name} AS A WHERE 1=1 {and} ORDER BY {SqlField.PrimaryKey} LIMIT {pageSize}", param);
        }

        public override IEnumerable<dynamic> GetByAscPrevPageDynamic(int pageSize, T param, string and = null, string returnFields = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return DpEntity.Query($"SELECT * FROM (SELECT {returnFields} FROM {Name} AS A WHERE {SqlField.PrimaryKey}<@{SqlField.PrimaryKey} {and} ORDER BY {SqlField.PrimaryKey} DESC LIMIT {pageSize}) AS B ORDER BY {SqlField.PrimaryKey}", param);
        }

        public override IEnumerable<dynamic> GetByAscCurrentPageDynamic(int pageSize, T param, string and = null, string returnFields = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return DpEntity.Query($"SELECT {returnFields} FROM {Name} AS A WHERE {SqlField.PrimaryKey}>=@{SqlField.PrimaryKey} {and} ORDER BY {SqlField.PrimaryKey} LIMIT {pageSize}", param);
        }

        public override IEnumerable<dynamic> GetByAscNextPageDynamic(int pageSize, T param, string and = null, string returnFields = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return DpEntity.Query($"SELECT {returnFields} FROM {Name} AS A WHERE {SqlField.PrimaryKey}>@{SqlField.PrimaryKey} {and} ORDER BY {SqlField.PrimaryKey} LIMIT {pageSize}", param);
        }

        public override IEnumerable<dynamic> GetByAscLastPageDynamic(int pageSize, object param = null, string and = null, string returnFields = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return DpEntity.Query($"SELECT * FROM (SELECT {returnFields} FROM {Name} AS A WHERE 1=1 {and} ORDER BY {SqlField.PrimaryKey} DESC LIMIT {pageSize}) AS B ORDER BY {SqlField.PrimaryKey}", param);
        }

        public override IEnumerable<dynamic> GetByDescFirstPageDynamic(int pageSize, object param = null, string and = null, string returnFields = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return DpEntity.Query($"SELECT {returnFields} FROM {Name} AS A WHERE 1=1 {and} ORDER BY {SqlField.PrimaryKey} DESC LIMIT {pageSize}", param);
        }

        public override IEnumerable<dynamic> GetByDescPrevPageDynamic(int pageSize, T param, string and = null, string returnFields = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return DpEntity.Query($"SELECT * FROM (SELECT {returnFields} FROM {Name} AS A WHERE {SqlField.PrimaryKey}>@{SqlField.PrimaryKey} {and} ORDER BY {SqlField.PrimaryKey} LIMIT {pageSize}) AS B ORDER BY {SqlField.PrimaryKey} DESC", param);
        }

        public override IEnumerable<dynamic> GetByDescCurrentPageDynamic(int pageSize, T param, string and = null, string returnFields = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return DpEntity.Query($"SELECT {returnFields} FROM {Name} AS A WHERE {SqlField.PrimaryKey}<=@{SqlField.PrimaryKey} {and} ORDER BY {SqlField.PrimaryKey} DESC LIMIT {pageSize}", param);
        }

        public override IEnumerable<dynamic> GetByDescNextPageDynamic(int pageSize, T param, string and = null, string returnFields = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return DpEntity.Query($"SELECT {returnFields} FROM {Name} AS A WHERE {SqlField.PrimaryKey}<@{SqlField.PrimaryKey} {and} ORDER BY {SqlField.PrimaryKey} DESC LIMIT {pageSize}", param);
        }

        public override IEnumerable<dynamic> GetByDescLastPageDynamic(int pageSize, object param = null, string and = null, string returnFields = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return DpEntity.Query($"SELECT * FROM (SELECT {returnFields} FROM {Name} AS A WHERE 1=1 {and} ORDER BY {SqlField.PrimaryKey} LIMIT {pageSize}) AS B ORDER BY {SqlField.PrimaryKey} DESC", param);
        }

        #endregion

    }
}
