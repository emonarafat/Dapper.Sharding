using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Dynamic.Core;

namespace Dapper.Sharding
{
    internal partial class ClickHouseTable<T> : ITable<T> where T : class
    {
        public ClickHouseTable(string name, IDatabase database) : base(name, database, SqlFieldCacheUtils.GetClickHouseFieldEntity<T>())
        {

        }

        private IEnumerable<T> GetInsertList(IEnumerable<T> modelList)
        {
            if (!modelList.Any())
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
            var data = DataBase.Query(sql, dpar);
            var existsIds = data.Select(s => ((IDictionary<string, object>)s).First().Value);

            var insertIds = ids.Except(existsIds);
            if (!insertIds.Any())
            {
                return Enumerable.Empty<T>();
            }
            return query.Where($"@0.Contains({SqlField.PrimaryKey})", insertIds).ToList();
        }

        #region insert NotImplementedException

        public override void InsertIdentity(T model, List<string> fields, DistributedTransaction tran = null, int? timeout = null)
        {
            throw new NotImplementedException();
        }

        public override void InsertIdentityIgnore(T model, List<string> fields, DistributedTransaction tran = null, int? timeout = null)
        {
            throw new NotImplementedException();
        }

        public override void InsertIdentity(IEnumerable<T> modelList, DistributedTransaction tran = null, int? timeout = null)
        {
            throw new NotImplementedException();
        }

        public override void InsertIdentity(IEnumerable<T> modelList, List<string> fields, DistributedTransaction tran = null, int? timeout = null)
        {
            throw new NotImplementedException();
        }

        public override void InsertIdentityIgnore(IEnumerable<T> modelList, List<string> fields, DistributedTransaction tran = null, int? timeout = null)
        {
            throw new NotImplementedException();
        }

        public override void InsertIdentityIfNoExists(T model, DistributedTransaction tran = null, int? timeout = null)
        {
            throw new NotImplementedException();
        }

        public override void InsertIdentityIfNoExists(T model, List<string> fields, DistributedTransaction tran = null, int? timeout = null)
        {
            throw new NotImplementedException();
        }

        public override void InsertIdentityIfNoExistsIgnore(T model, List<string> fields, DistributedTransaction tran = null, int? timeout = null)
        {
            throw new NotImplementedException();
        }

        public override void InsertIdentityIfNoExists(IEnumerable<T> modelList, DistributedTransaction tran = null, int? timeout = null)
        {
            throw new NotImplementedException();
        }

        public override void InsertIdentityIfNoExists(IEnumerable<T> modelList, List<string> fields, DistributedTransaction tran = null, int? timeout = null)
        {
            throw new NotImplementedException();
        }

        public override void InsertIdentityIfNoExistsIgnore(IEnumerable<T> modelList, List<string> fields, DistributedTransaction tran = null, int? timeout = null)
        {
            throw new NotImplementedException();
        }

        public override void Merge(T model, List<string> fields = null, DistributedTransaction tran = null, int? timeout = null)
        {
            throw new NotImplementedException();
        }

        public override void Merge(IEnumerable<T> modelList, List<string> fields = null, DistributedTransaction tran = null, int? timeout = null)
        {
            throw new NotImplementedException();
        }

        public override void MergeIgnore(T model, List<string> fields = null, DistributedTransaction tran = null, int? timeout = null)
        {
            throw new NotImplementedException();
        }

        public override void MergeIgnore(IEnumerable<T> modelList, List<string> fields = null, DistributedTransaction tran = null, int? timeout = null)
        {
            throw new NotImplementedException();
        }

        #endregion

        #region insert

        protected override string SqlInsert()
        {
            return $"INSERT INTO {Name} ({SqlField.AllFields}) SELECT {SqlField.AllFieldsAt}";
        }

        protected override string SqlInsertIdentity()
        {
            return $"INSERT INTO {Name} ({SqlField.AllFields}) SELECT {SqlField.AllFieldsAt}";
        }

        public override void Insert(T model, List<string> fields, DistributedTransaction tran = null, int? timeout = null)
        {
            var ff = CommonUtil.GetFieldsStr(fields, "", "");
            var vv = CommonUtil.GetFieldsAtStr(fields, "@");
            var sql = $"INSERT INTO {Name} ({ff}) SELECT {vv}";
            DataBase.Execute(sql, model, null, timeout);
        }

        public override void InsertIgnore(T model, List<string> fields, DistributedTransaction tran = null, int? timeout = null)
        {
            var exList = SqlField.AllFieldList.Except(fields).ToList();
            var ff = CommonUtil.GetFieldsStr(exList, "", "");
            var vv = CommonUtil.GetFieldsAtStr(exList, "@");
            var sql = $"INSERT INTO {Name} ({ff}) SELECT {vv}";
            DataBase.Execute(sql, model, null, timeout);

        }

        public override void Insert(IEnumerable<T> modelList, DistributedTransaction tran = null, int? timeout = null)
        {
            var sql = $"INSERT INTO {Name} ({SqlField.AllFields}) VALUES @list";
            DataBase.Execute(sql, new { list = modelList }, null, timeout);
        }

        public override void Insert(IEnumerable<T> modelList, List<string> fields, DistributedTransaction tran = null, int? timeout = null)
        {
            var ff = CommonUtil.GetFieldsStr(fields, "", "");
            var sql = $"INSERT INTO {Name} ({ff}) VALUES @list";
            DataBase.Execute(sql, new { list = modelList }, null, timeout);
        }

        public override void InsertIgnore(IEnumerable<T> modelList, List<string> fields, DistributedTransaction tran = null, int? timeout = null)
        {
            var exList = SqlField.AllFieldList.Except(fields).ToList();
            var ff = CommonUtil.GetFieldsStr(exList, "", "");
            var sql = $"INSERT INTO {Name} ({ff}) VALUES @list";
            DataBase.Execute(sql, new { list = modelList }, null, timeout);
        }

        public override void InsertIfNoExists(T model, DistributedTransaction tran = null, int? timeout = null)
        {
            if (!Exists(model))
            {
                Insert(model, null, timeout);
            }
        }

        public override void InsertIfNoExists(T model, List<string> fields, DistributedTransaction tran = null, int? timeout = null)
        {
            if (!Exists(model))
            {
                Insert(model, fields, null, timeout);
            }
        }

        public override void InsertIfNoExistsIgnore(T model, List<string> fields, DistributedTransaction tran = null, int? timeout = null)
        {
            if (!Exists(model))
            {
                InsertIgnore(model, fields, null, timeout);
            }
        }

        public override void InsertIfNoExists(IEnumerable<T> modelList, DistributedTransaction tran = null, int? timeout = null)
        {
            var list = GetInsertList(modelList);
            if (list.Any())
            {
                Insert(list, null, timeout);
            }
        }

        public override void InsertIfNoExists(IEnumerable<T> modelList, List<string> fields, DistributedTransaction tran = null, int? timeout = null)
        {
            var list = GetInsertList(modelList);
            if (list.Any())
            {
                Insert(list, fields, null, timeout);
            }
        }

        public override void InsertIfNoExistsIgnore(IEnumerable<T> modelList, List<string> fields, DistributedTransaction tran = null, int? timeout = null)
        {
            var list = GetInsertList(modelList);
            if (list.Any())
            {
                InsertIgnore(list, fields, null, timeout);
            }
        }

        #endregion

        #region update

        protected override string SqlUpdate(List<string> fields = null)
        {
            string updatefields;
            if (fields == null)
                updatefields = SqlField.AllFieldsAtEqExceptKey;
            else
                updatefields = CommonUtil.GetFieldsAtEqStr(fields, "", "");
            return $"ALTER TABLE {Name} UPDATE {updatefields} WHERE {SqlField.PrimaryKey}=@{SqlField.PrimaryKey}";
        }

        protected override string SqlUpdateIgnore(List<string> fields)
        {
            string updateFields = CommonUtil.GetFieldsAtEqStr(SqlField.AllFieldExceptKeyList.Except(fields), "", "");
            return $"ALTER TABLE {Name} UPDATE {updateFields} WHERE {SqlField.PrimaryKey}=@{SqlField.PrimaryKey}";
        }

        protected override string SqlUpdateByWhere(string where, List<string> fields = null)
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
            return $"ALTER TABLE {Name} UPDATE {updatefields} {where}";
        }

        protected override string SqlUpdateByWhereIgnore(string where, List<string> fields)
        {
            string updateFields = CommonUtil.GetFieldsAtEqStr(SqlField.AllFieldExceptKeyList.Except(fields), "", "");
            return $"ALTER TABLE {Name} UPDATE {updateFields} {where}";
        }

        public override void Update(IEnumerable<T> modelList, List<string> fields = null, DistributedTransaction tran = null, int? timeout = null)
        {
            foreach (var item in modelList)
            {
                Update(item, fields);
            }
        }

        public override void UpdateIgnore(IEnumerable<T> modelList, List<string> fields, DistributedTransaction tran = null, int? timeout = null)
        {
            foreach (var item in modelList)
            {
                UpdateIgnore(item, fields);
            }
        }

        #endregion

        #region delete

        protected override string SqlDeleteById()
        {
            return $"ALTER TABLE {Name} DELETE WHERE {SqlField.PrimaryKey}=@id";
        }

        protected override string SqlDeleteByIds()
        {
            return $"ALTER TABLE {Name} DELETE WHERE {SqlField.PrimaryKey} IN (@ids)";
        }

        protected override string SqlDeleteByWhere(string where)
        {
            return $"ALTER TABLE {Name} DELETE {where}";
        }

        protected override string SqlDeleteAll()
        {
            DataBase.DropTable(Name);
            DataBase.CreateTable<T>(Name);
            return null;
        }

        public override void Delete(IEnumerable<T> modelList)
        {
            if (!modelList.Any())
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
            DataBase.Execute(sql, dpar);
        }



        #endregion

        #region aggregate

        protected override string SqlExists()
        {
            return $"SELECT 1 FROM {Name} WHERE {SqlField.PrimaryKey}=@id";
        }

        protected override string SqlCount(string where = null)
        {
            if (!string.IsNullOrEmpty(SqlField.PrimaryKey))
            {
                return $"SELECT COUNT({SqlField.PrimaryKey}) FROM {Name} {where}";
            }
            return $"SELECT COUNT() FROM {Name} {where}";
        }

        protected override string SqlMin(string field, string where = null)
        {
            return $"SELECT MIN({field}) FROM {Name} {where}";
        }

        protected override string SqlMax(string field, string where = null)
        {
            return $"SELECT MAX({field}) FROM {Name} {where}";
        }

        protected override string SqlSum(string field, string where = null)
        {
            return $"SELECT SUM({field}) FROM {Name} {where}";
        }

        protected override string SqlAvg(string field, string where = null)
        {
            return $"SELECT AVG({field}) FROM {Name} {where}";
        }

        #endregion

        #region query

        protected override string SqlGetAll(string returnFields = null, string orderby = null, bool dy = false)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return $"SELECT {returnFields} FROM {Name} {orderby.SetOrderBy(SqlField.PrimaryKey)}";
        }

        public override string SqlGetById(string returnFields = null, bool dy = false)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return $"SELECT {returnFields} FROM {Name} WHERE {SqlField.PrimaryKey}=@id";
        }

        public override string SqlGetByIdForUpdate(string returnFields = null, bool dy = false)
        {
            throw new NotImplementedException();
        }

        public override string SqlGetByIds(string returnFields = null, bool dy = false)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return $"SELECT {returnFields} FROM {Name} WHERE {SqlField.PrimaryKey} IN (@ids)";
        }

        public override string SqlGetByIdsForUpdate(string returnFields = null, bool dy = false)
        {
            throw new NotImplementedException();
        }

        public override string SqlGetByIdsWithField(string field, string returnFields = null, bool dy = false)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return $"SELECT {returnFields} FROM {Name} WHERE {field} IN (@ids)";
        }

        public override string SqlGetByWhere(string where, string returnFields = null, string orderby = null, int limit = 0, bool dy = false)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            string limitStr = null;
            if (limit != 0)
            {
                limitStr = "LIMIT " + limit;
            }
            return $"SELECT {returnFields} FROM {Name} {where} {orderby.SetOrderBy(SqlField.PrimaryKey)} {limitStr}";
        }

        public override string SqlGetByWhereFirst(string where, string returnFields = null, bool dy = false)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return $"SELECT {returnFields} FROM {Name} {where} LIMIT 1";
        }

        public override string SqlGetBySkipTake(int skip, int take, string where = null, string returnFields = null, string orderby = null, bool dy = false)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return $"SELECT {returnFields} FROM {Name} {where} {orderby.SetOrderBy(SqlField.PrimaryKey)} LIMIT {skip},{take}";
        }

        public override string SqlGetByAscFirstPage(int pageSize, string and = null, string returnFields = null, bool dy = false)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return $"SELECT {returnFields} FROM {Name} AS A WHERE 1=1 {and} ORDER BY {SqlField.PrimaryKey} LIMIT {pageSize}";
        }

        public override string SqlGetByAscPrevPage(int pageSize, string and = null, string returnFields = null, bool dy = false)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return $"SELECT * FROM (SELECT {returnFields} FROM {Name} AS A WHERE {SqlField.PrimaryKey}<@{SqlField.PrimaryKey} {and} ORDER BY {SqlField.PrimaryKey} DESC LIMIT {pageSize}) AS B ORDER BY {SqlField.PrimaryKey}";
        }

        public override string SqlGetByAscCurrentPage(int pageSize, string and = null, string returnFields = null, bool dy = false)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return $"SELECT {returnFields} FROM {Name} AS A WHERE {SqlField.PrimaryKey}>=@{SqlField.PrimaryKey} {and} ORDER BY {SqlField.PrimaryKey} LIMIT {pageSize}";
        }

        public override string SqlGetByAscNextPage(int pageSize, string and = null, string returnFields = null, bool dy = false)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return $"SELECT {returnFields} FROM {Name} AS A WHERE {SqlField.PrimaryKey}>@{SqlField.PrimaryKey} {and} ORDER BY {SqlField.PrimaryKey} LIMIT {pageSize}";
        }

        public override string SqlGetByAscLastPage(int pageSize, string and = null, string returnFields = null, bool dy = false)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return $"SELECT * FROM (SELECT {returnFields} FROM {Name} AS A WHERE 1=1 {and} ORDER BY {SqlField.PrimaryKey} DESC LIMIT {pageSize}) AS B ORDER BY {SqlField.PrimaryKey}";
        }

        public override string SqlGetByDescFirstPage(int pageSize, string and = null, string returnFields = null, bool dy = false)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return $"SELECT {returnFields} FROM {Name} AS A WHERE 1=1 {and} ORDER BY {SqlField.PrimaryKey} DESC LIMIT {pageSize}";
        }

        public override string SqlGetByDescPrevPage(int pageSize, string and = null, string returnFields = null, bool dy = false)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return $"SELECT * FROM (SELECT {returnFields} FROM {Name} AS A WHERE {SqlField.PrimaryKey}>@{SqlField.PrimaryKey} {and} ORDER BY {SqlField.PrimaryKey} LIMIT {pageSize}) AS B ORDER BY {SqlField.PrimaryKey} DESC";
        }

        public override string SqlGetByDescCurrentPage(int pageSize, string and = null, string returnFields = null, bool dy = false)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return $"SELECT {returnFields} FROM {Name} AS A WHERE {SqlField.PrimaryKey}<=@{SqlField.PrimaryKey} {and} ORDER BY {SqlField.PrimaryKey} DESC LIMIT {pageSize}";
        }

        public override string SqlGetByDescNextPage(int pageSize, string and = null, string returnFields = null, bool dy = false)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return $"SELECT {returnFields} FROM {Name} AS A WHERE {SqlField.PrimaryKey}<@{SqlField.PrimaryKey} {and} ORDER BY {SqlField.PrimaryKey} DESC LIMIT {pageSize}";
        }

        public override string SqlGetByDescLastPage(int pageSize, string and = null, string returnFields = null, bool dy = false)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return $"SELECT * FROM (SELECT {returnFields} FROM {Name} AS A WHERE 1=1 {and} ORDER BY {SqlField.PrimaryKey} LIMIT {pageSize}) AS B ORDER BY {SqlField.PrimaryKey} DESC";
        }

        #endregion
    }
}
