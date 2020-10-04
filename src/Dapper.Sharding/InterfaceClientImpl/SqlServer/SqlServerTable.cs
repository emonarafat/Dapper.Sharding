using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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
            var accessor = TypeAccessor.Create(typeof(T));
            if (SqlField.IsIdentity)
            {
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

        public override TValue Avg<TValue>(string field, string where = null, object param = null)
        {
            throw new NotImplementedException();
        }

        public override long Count()
        {
            throw new NotImplementedException();
        }

        public override long Count(string where, object param = null)
        {
            throw new NotImplementedException();
        }


        public override bool Delete(object id)
        {
            throw new NotImplementedException();
        }

        public override int DeleteAll()
        {
            throw new NotImplementedException();
        }

        public override int DeleteByIds(object ids)
        {
            throw new NotImplementedException();
        }

        public override int DeleteByWhere(string where, object param = null)
        {
            throw new NotImplementedException();
        }

        public override bool Exists(object id)
        {
            throw new NotImplementedException();
        }

        public override IEnumerable<T> GetAll(string returnFields = null, string orderby = null)
        {
            throw new NotImplementedException();
        }

        public override IEnumerable<T> GetByAscCurrentPage(int pageSize, T param, string and = null, string returnFields = null)
        {
            throw new NotImplementedException();
        }

        public override IEnumerable<T> GetByAscFirstPage(int pageSize, object param = null, string and = null, string returnFields = null)
        {
            throw new NotImplementedException();
        }

        public override IEnumerable<T> GetByAscLastPage(int pageSize, object param = null, string and = null, string returnFields = null)
        {
            throw new NotImplementedException();
        }

        public override IEnumerable<T> GetByAscNextPage(int pageSize, T param, string and = null, string returnFields = null)
        {
            throw new NotImplementedException();
        }

        public override IEnumerable<T> GetByAscPrevPage(int pageSize, T param, string and = null, string returnFields = null)
        {
            throw new NotImplementedException();
        }

        public override IEnumerable<T> GetByDescCurrentPage(int pageSize, T param, string and = null, string returnFields = null)
        {
            throw new NotImplementedException();
        }

        public override IEnumerable<T> GetByDescFirstPage(int pageSize, object param = null, string and = null, string returnFields = null)
        {
            throw new NotImplementedException();
        }

        public override IEnumerable<T> GetByDescLastPage(int pageSize, object param = null, string and = null, string returnFields = null)
        {
            throw new NotImplementedException();
        }

        public override IEnumerable<T> GetByDescNextPage(int pageSize, T param, string and = null, string returnFields = null)
        {
            throw new NotImplementedException();
        }

        public override IEnumerable<T> GetByDescPrevPage(int pageSize, T param, string and = null, string returnFields = null)
        {
            throw new NotImplementedException();
        }

        public override T GetById(object id, string returnFields = null)
        {
            throw new NotImplementedException();
        }

        public override T GetByIdForUpdate(object id, string returnFields = null)
        {
            throw new NotImplementedException();
        }

        public override IEnumerable<T> GetByIds(object ids, string returnFields = null)
        {
            throw new NotImplementedException();
        }

        public override IEnumerable<T> GetByIdsForUpdate(object ids, string returnFields = null)
        {
            throw new NotImplementedException();
        }

        public override IEnumerable<T> GetByIdsWithField(object ids, string field, string returnFields = null)
        {
            throw new NotImplementedException();
        }

        public override IEnumerable<T> GetBySkipTake(int skip, int take, string where = null, object param = null, string returnFields = null, string orderby = null)
        {
            throw new NotImplementedException();
        }

        public override IEnumerable<T> GetByWhere(string where, object param = null, string returnFields = null, string orderby = null, int limit = 0)
        {
            throw new NotImplementedException();
        }

        public override T GetByWhereFirst(string where, object param = null, string returnFields = null)
        {
            throw new NotImplementedException();
        }

        public override TValue Max<TValue>(string field, string where = null, object param = null)
        {
            throw new NotImplementedException();
        }

        public override TValue Min<TValue>(string field, string where = null, object param = null)
        {
            throw new NotImplementedException();
        }

        public override TValue Sum<TValue>(string field, string where = null, object param = null)
        {
            throw new NotImplementedException();
        }

        public override void Truncate()
        {
            throw new NotImplementedException();
        }

        public override int UpdateByWhere(T model, string where, List<string> fields = null)
        {
            throw new NotImplementedException();
        }

        public override int UpdateByWhereIgnore(T model, string where, List<string> fields)
        {
            throw new NotImplementedException();
        }
    }
}
