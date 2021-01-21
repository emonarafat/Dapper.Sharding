using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace Dapper.Sharding
{
    internal class SqlServerTable<T> : ITable<T> where T : class
    {
        public SqlServerTable(string name, IDatabase database, IDbConnection conn = null, IDbTransaction tran = null, int? commandTimeout = null) : base(name, database, SqlFieldCacheUtils.GetSqlServerFieldEntity<T>(), new DapperEntity(name, database, conn, tran, commandTimeout))
        {

        }

        public override ITable<T> CreateTranTable(IDbConnection conn, IDbTransaction tran, int? commandTimeout = null)
        {
            return new SqlServerTable<T>(Name, DataBase, conn, tran, commandTimeout);
        }

        #region virtual

        public override bool Insert(T model)
        {
            if (SqlField.IsIdentity)
            {
                var accessor = TypeAccessor.Create(typeof(T));
                var sql = $"INSERT INTO [{Name}] ({SqlField.AllFieldsExceptKey})VALUES({SqlField.AllFieldsAtExceptKey});SELECT @@IDENTITY";
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
                var sql = $"INSERT INTO [{Name}] ({SqlField.AllFields})VALUES({SqlField.AllFieldsAt})";
                return DpEntity.Execute(sql, model) > 0;
            }
        }

        public override bool InsertIdentity(T model)
        {
            return DpEntity.Execute($"INSERT INTO [{Name}] ({SqlField.AllFields})VALUES({SqlField.AllFieldsAt})", model) > 0;
        }

        public override bool Update(T model, List<string> fields = null)
        {
            string updatefields;
            if (fields == null)
                updatefields = SqlField.AllFieldsAtEqExceptKey;
            else
                updatefields = CommonUtil.GetFieldsAtEqStr(fields, "[", "]");
            return DpEntity.Execute($"UPDATE [{Name}] SET {updatefields} WHERE [{SqlField.PrimaryKey}]=@{SqlField.PrimaryKey}", model) > 0;
        }

        public override bool UpdateIgnore(T model, List<string> fields)
        {
            string updateFields = CommonUtil.GetFieldsAtEqStr(SqlField.AllFieldExceptKeyList.Except(fields), "[", "]");
            return DpEntity.Execute($"UPDATE [{Name}] SET {updateFields} WHERE [{SqlField.PrimaryKey}]=@{SqlField.PrimaryKey}", model) > 0;
        }

        #endregion

        public override decimal Avg(string field, string where = null, object param = null)
        {
            return DpEntity.ExecuteScalar<decimal>($"SELECT AVG([{field}]) FROM [{Name}] {where}", param);
        }

        public override long Count(string where, object param = null)
        {
            return DpEntity.ExecuteScalar<long>($"SELECT COUNT(1) FROM [{Name}] {where}", param);
        }


        public override bool Delete(object id)
        {
            return DpEntity.Execute($"DELETE FROM [{Name}] WHERE [{SqlField.PrimaryKey}]=@id", new { id }) > 0;
        }

        public override int DeleteAll()
        {
            return DpEntity.Execute($"DELETE FROM [{Name}]");
        }

        public override int DeleteByIds(object ids)
        {
            if (CommonUtil.ObjectIsEmpty(ids))
                return 0;
            var dpar = new DynamicParameters();
            dpar.Add("@ids", ids);
            return DpEntity.Execute($"DELETE FROM [{Name}] WHERE [{SqlField.PrimaryKey}] IN @ids", dpar);
        }

        public override int DeleteByWhere(string where, object param = null)
        {
            return DpEntity.Execute($"DELETE FROM [{Name}] {where}", param);
        }

        public override bool Exists(object id)
        {
            return DpEntity.ExecuteScalar($"SELECT 1 FROM [{Name}] WHERE [{SqlField.PrimaryKey}]=@id", new { id }) != null;
        }

        public override IEnumerable<T> GetAll(string returnFields = null, string orderby = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return DpEntity.Query<T>($"SELECT {returnFields} FROM [{Name}] {orderby.SetOrderBy(SqlField.PrimaryKey)}");
        }

        public override IEnumerable<T> GetByAscFirstPage(int pageSize, object param = null, string and = null, string returnFields = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return DpEntity.Query<T>($"SELECT TOP({pageSize}) {returnFields} FROM [{Name}] AS A WHERE 1=1 {and} ORDER BY [{SqlField.PrimaryKey}]", param);
        }

        public override IEnumerable<T> GetByAscCurrentPage(int pageSize, T param, string and = null, string returnFields = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return DpEntity.Query<T>($"SELECT TOP({pageSize}) {returnFields} FROM [{Name}] AS A WHERE [{SqlField.PrimaryKey}]>=@{SqlField.PrimaryKey} {and} ORDER BY [{SqlField.PrimaryKey}]", param);
        }

        public override IEnumerable<T> GetByAscLastPage(int pageSize, object param = null, string and = null, string returnFields = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return DpEntity.Query<T>($"SELECT * FROM (SELECT TOP({pageSize}) {returnFields} FROM [{Name}] AS A WHERE 1=1 {and} ORDER BY [{SqlField.PrimaryKey}] DESC) AS B ORDER BY [{SqlField.PrimaryKey}]", param);
        }

        public override IEnumerable<T> GetByAscNextPage(int pageSize, T param, string and = null, string returnFields = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return DpEntity.Query<T>($"SELECT TOP({pageSize}) {returnFields} FROM [{Name}] AS A WHERE [{SqlField.PrimaryKey}]>@{SqlField.PrimaryKey} {and} ORDER BY [{SqlField.PrimaryKey}]", param);
        }

        public override IEnumerable<T> GetByAscPrevPage(int pageSize, T param, string and = null, string returnFields = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return DpEntity.Query<T>($"SELECT * FROM (SELECT TOP({pageSize}) {returnFields} FROM [{Name}] AS A WHERE [{SqlField.PrimaryKey}]<@{SqlField.PrimaryKey} {and} ORDER BY [{SqlField.PrimaryKey}] DESC) AS B ORDER BY [{SqlField.PrimaryKey}]", param);
        }

        public override IEnumerable<T> GetByDescCurrentPage(int pageSize, T param, string and = null, string returnFields = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return DpEntity.Query<T>($"SELECT TOP({pageSize}) {returnFields} FROM [{Name}] AS A WHERE [{SqlField.PrimaryKey}]<=@{SqlField.PrimaryKey} {and} ORDER BY [{SqlField.PrimaryKey}] DESC", param);
        }

        public override IEnumerable<T> GetByDescFirstPage(int pageSize, object param = null, string and = null, string returnFields = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return DpEntity.Query<T>($"SELECT TOP({pageSize}) {returnFields} FROM [{Name}] AS A WHERE 1=1 {and} ORDER BY [{SqlField.PrimaryKey}] DESC", param);
        }

        public override IEnumerable<T> GetByDescLastPage(int pageSize, object param = null, string and = null, string returnFields = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return DpEntity.Query<T>($"SELECT * FROM (SELECT TOP({pageSize}) {returnFields} FROM [{Name}] AS A WHERE 1=1 {and} ORDER BY [{SqlField.PrimaryKey}]) AS B ORDER BY [{SqlField.PrimaryKey}] DESC", param);
        }

        public override IEnumerable<T> GetByDescNextPage(int pageSize, T param, string and = null, string returnFields = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return DpEntity.Query<T>($"SELECT TOP({pageSize}) {returnFields} FROM [{Name}] AS A WHERE [{SqlField.PrimaryKey}]<@{SqlField.PrimaryKey} {and} ORDER BY [{SqlField.PrimaryKey}] DESC", param);
        }

        public override IEnumerable<T> GetByDescPrevPage(int pageSize, T param, string and = null, string returnFields = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return DpEntity.Query<T>($"SELECT * FROM (SELECT TOP({pageSize}) {returnFields} FROM [{Name}] AS A WHERE [{SqlField.PrimaryKey}]>@{SqlField.PrimaryKey} {and} ORDER BY [{SqlField.PrimaryKey}]) AS B ORDER BY [{SqlField.PrimaryKey}] DESC", param);
        }

        public override T GetById(object id, string returnFields = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return DpEntity.QueryFirstOrDefault<T>($"SELECT {returnFields} FROM [{Name}] WHERE [{SqlField.PrimaryKey}]=@id", new { id });
        }

        public override T GetByIdForUpdate(object id, string returnFields = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return DpEntity.QueryFirstOrDefault<T>($"SELECT {returnFields} FROM [{Name}] WITH (UPDLOCK) WHERE [{SqlField.PrimaryKey}]=@id", new { id });
        }

        public override IEnumerable<T> GetByIds(object ids, string returnFields = null)
        {
            if (CommonUtil.ObjectIsEmpty(ids))
                return Enumerable.Empty<T>();
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            var dpar = new DynamicParameters();
            dpar.Add("@ids", ids);
            return DpEntity.Query<T>($"SELECT {returnFields} FROM [{Name}] WHERE [{SqlField.PrimaryKey}] IN @ids", dpar);
        }

        public override IEnumerable<T> GetByIdsForUpdate(object ids, string returnFields = null)
        {
            if (CommonUtil.ObjectIsEmpty(ids))
                return Enumerable.Empty<T>();
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            var dpar = new DynamicParameters();
            dpar.Add("@ids", ids);
            return DpEntity.Query<T>($"SELECT {returnFields} FROM [{Name}] WITH (UPDLOCK) WHERE [{SqlField.PrimaryKey}] IN @ids", dpar);
        }

        public override IEnumerable<T> GetByIdsWithField(object ids, string field, string returnFields = null)
        {
            if (CommonUtil.ObjectIsEmpty(ids))
                return Enumerable.Empty<T>();
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            var dpar = new DynamicParameters();
            dpar.Add("@ids", ids);
            return DpEntity.Query<T>($"SELECT {returnFields} FROM [{Name}] WHERE [{field}] IN @ids", dpar);
        }

        public override IEnumerable<T> GetBySkipTake(int skip, int take, string where = null, object param = null, string returnFields = null, string orderby = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            if (DataBase.Client.DbType == DataBaseType.SqlServer2012)
            {
                return DpEntity.Query<T>($"SELECT {returnFields} FROM [{Name}] {where} {orderby.SetOrderBy(SqlField.PrimaryKey)} offset {skip} rows fetch next {take} rows only", param);
            }
            else
            {
                if (skip == 0) //第一页,使用Top语句
                {
                    return DpEntity.Query<T>($"SELECT TOP ({take}) {returnFields} FROM [{Name}] {where} {orderby.SetOrderBy(SqlField.PrimaryKey)}", param);
                }
                else //使用ROW_NUMBER()
                {
                    return DpEntity.Query<T>($"WITH cte AS(SELECT ROW_NUMBER() OVER({orderby.SetOrderBy(SqlField.PrimaryKey)}) AS Row_Number,{returnFields} FROM [{Name}] {where}) SELECT * FROM cte WHERE cte.Row_Number BETWEEN {skip + 1} AND {skip + take}", param);
                }
            }

        }

        public override IEnumerable<T> GetByWhere(string where, object param = null, string returnFields = null, string orderby = null, int limit = 0)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            string top = null;
            if (limit != 0)
            {
                top = $"TOP({limit}) ";
            }
            return DpEntity.Query<T>($"SELECT {top} {returnFields} FROM [{Name}] {where} {orderby.SetOrderBy(SqlField.PrimaryKey)}", param);
        }

        public override T GetByWhereFirst(string where, object param = null, string returnFields = null)
        {
            if (string.IsNullOrEmpty(returnFields))
                returnFields = SqlField.AllFields;
            return DpEntity.QueryFirstOrDefault<T>($"SELECT TOP(1) {returnFields} FROM [{Name}] {where}", param);
        }

        public override TValue Max<TValue>(string field, string where = null, object param = null)
        {
            return DpEntity.ExecuteScalar<TValue>($"SELECT MAX([{field}]) FROM [{Name}] {where}", param);
        }

        public override TValue Min<TValue>(string field, string where = null, object param = null)
        {
            return DpEntity.ExecuteScalar<TValue>($"SELECT MIN([{field}]) FROM [{Name}] {where}", param);
        }

        public override TValue Sum<TValue>(string field, string where = null, object param = null)
        {
            return DpEntity.ExecuteScalar<TValue>($"SELECT SUM([{field}]) FROM [{Name}] {where}", param);
        }

        public override void Truncate()
        {
            DataBase.TruncateTable(Name);
        }

        public override int UpdateByWhere(T model, string where, List<string> fields = null)
        {
            string updatefields;
            if (fields != null)
            {
                updatefields = CommonUtil.GetFieldsAtEqStr(fields, "[", "]");
            }
            else
            {
                updatefields = SqlField.AllFieldsAtEqExceptKey;
            }
            return DpEntity.Execute($"UPDATE [{Name}] SET {updatefields} {where}", model);
        }

        public override int UpdateByWhereIgnore(T model, string where, List<string> fields)
        {
            string updatefields = CommonUtil.GetFieldsAtEqStr(SqlField.AllFieldExceptKeyList.Except(fields), "[", "]");
            return DpEntity.Execute($"UPDATE [{Name}] SET {updatefields} {where}", model);
        }


        public override int UpdateByWhere(string where, object param, List<string> fields = null)
        {
            string updatefields;
            if (fields != null)
            {
                updatefields = CommonUtil.GetFieldsAtEqStr(fields, "[", "]");
            }
            else
            {
                updatefields = SqlField.AllFieldsAtEqExceptKey;
            }
            return DpEntity.Execute($"UPDATE [{Name}] SET {updatefields} {where}", param);
        }
    }
}
