﻿using System;
using System.Collections.Generic;
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

        #region update

        protected override string SqlUpdate(List<string> fields = null)
        {
            string updatefields;
            if (fields == null)
                updatefields = SqlField.AllFieldsAtEqExceptKey;
            else
                updatefields = CommonUtil.GetFieldsAtEqStr(fields, "", "");
            return $"UPDATE {Name} SET {updatefields} WHERE {SqlField.PrimaryKey}=@{SqlField.PrimaryKey}";
        }

        protected override string SqlUpdateIgnore(List<string> fields)
        {
            string updateFields = CommonUtil.GetFieldsAtEqStr(SqlField.AllFieldExceptKeyList.Except(fields), "", "");
            return $"UPDATE {Name} SET {updateFields} WHERE {SqlField.PrimaryKey}=@{SqlField.PrimaryKey}";
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
            return $"UPDATE {Name} SET {updatefields} {where}";
        }

        protected override string SqlUpdateByWhereIgnore(string where, List<string> fields)
        {
            string updatefields = CommonUtil.GetFieldsAtEqStr(SqlField.AllFieldExceptKeyList.Except(fields), "", "");
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
            return $"DELETE FROM {Name} WHERE {SqlField.PrimaryKey} IN @ids";
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

        #region aggregate

        protected override string SqlExists()
        {
            return $"SELECT 1 FROM {Name} WHERE {SqlField.PrimaryKey}=@id";
        }

        protected override string SqlCount(string where = null)
        {
            return $"SELECT COUNT(1) FROM {Name} {where}";
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
            return $"SELECT {returnFields} FROM {Name} WHERE {SqlField.PrimaryKey} IN @ids";
        }

        public override string SqlGetByIdsForUpdate(string returnFields = null, bool dy = false)
        {
            throw new NotImplementedException();
        }

        public override string SqlGetByIdsWithField(string field, string returnFields = null, bool dy = false)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return $"SELECT {returnFields} FROM {Name} WHERE {field} IN @ids";
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
