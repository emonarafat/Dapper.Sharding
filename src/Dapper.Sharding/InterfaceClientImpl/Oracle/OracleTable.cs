using System;
using System.Collections.Generic;
using System.Data;

namespace Dapper.Sharding
{
    internal class OracleTable<T> : ITable<T> where T : class
    {
        public OracleTable(string name, IDatabase database, IDbConnection conn = null, IDbTransaction tran = null, int? commandTimeout = null) : base(name, database, SqlFieldCacheUtils.GetOracleFieldEntity<T>(), new DapperEntity(name, database, conn, tran, commandTimeout))
        {

        }

        public override TValue Avg<TValue>(string field, string where = null, object param = null)
        {
            throw new NotImplementedException();
        }

        public override long Count(string where, object param = null)
        {
            throw new NotImplementedException();
        }

        public override ITable<T> CreateTranTable(IDbConnection conn, IDbTransaction tran, int? commandTimeout = null)
        {
            return new OracleTable<T>(Name, DataBase, conn, tran, commandTimeout);
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
