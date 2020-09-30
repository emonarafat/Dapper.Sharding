using System.Collections.Generic;
using System.Data;

namespace Dapper.Sharding
{
    public interface ITable<T> where T : class
    {
        string Name { get; }

        IDatabase DataBase { get; }

        DapperEntity DpEntity { get; }

        SqlFieldEntity SqlField { get; }

        ITable<T> CreateTranTable(IDbConnection conn, IDbTransaction tran, int? commandTimeout = null);

        bool Insert(T model);

        void BulkInsert(IEnumerable<T> modelList);

        bool InsertIdentity(T model);

        int InsertIdentityMany(IEnumerable<T> modelList);

        bool InsertIfNoExists(T model);

        bool InsertIfExistsUpdate(T model, string fields = null);

        bool InsertIdentityIfNoExists(T model);

        bool InsertIdentityIfExistsUpdate(T model, string fields = null);

        bool Update(T model);

        int UpdateMany(IEnumerable<T> modelList);

        bool UpdateInclude(T model, string fields);

        int UpdateIncludeMany(IEnumerable<T> modelList, string fields);

        bool UpdateExclude(T model, string fields);

        int UpdateExcludeMany(IEnumerable<T> modelList, string fields);

        int UpdateByWhere(T model, string where);

        int UpdateByWhereInclude(T model, string where, string fields);

        int UpdateByWhereExclude(T model, string where, string fields);

        bool Delete(object id);

        int DeleteByIds(object ids);

        int DeleteByWhere(string where, object param = null);

        int DeleteAll();

        void Truncate();

        bool Exists(object id);

        long Count();

        long Count(string where, object param = null);

        TValue Min<TValue>(string field, string where = null, object param = null);

        TValue Max<TValue>(string field, string where = null, object param = null);

        TValue Sum<TValue>(string field, string where = null, object param = null);

        TValue Avg<TValue>(string field, string where = null, object param = null);

        IEnumerable<T> GetAll(string returnFields = null, string orderby = null);

        T GetById(object id, string returnFields = null);

        T GetByIdForUpdate(object id, string returnFields = null);

        IEnumerable<T> GetByIds(object ids, string returnFields = null);

        IEnumerable<T> GetByIdsForUpdate(object ids, string returnFields = null);

        IEnumerable<T> GetByIdsWithField(object ids, string field, string returnFields = null);

        IEnumerable<T> GetByWhere(string where, object param = null, string returnFields = null, string orderby = null, int limit = 0);

        T GetByWhereFirst(string where, object param = null, string returnFields = null);

        IEnumerable<T> GetBySkipTake(int skip, int take, string where = null, object param = null, string returnFields = null, string orderby = null);

        IEnumerable<T> GetByPage(int page, int pageSize, string where = null, object param = null, string returnFields = null, string orderby = null);

        IEnumerable<T> GetByPageAndCount(int page, int pageSize, out long count, string where = null, object param = null, string returnFields = null, string orderby = null);

        IEnumerable<T> GetByAscFirstPage(int pageSize, object param = null, string and = null, string returnFields = null);

        IEnumerable<T> GetByAscPrevPage(int pageSize, T param, string and = null, string returnFields = null);

        IEnumerable<T> GetByAscCurrentPage(int pageSize, T param, string and = null, string returnFields = null);

        IEnumerable<T> GetByAscNextPage(int pageSize, T param, string and = null, string returnFields = null);

        IEnumerable<T> GetByAscLastPage(int pageSize, object param = null, string and = null, string returnFields = null);

        IEnumerable<T> GetByDescFirstPage(int pageSize, object param = null, string and = null, string returnFields = null);

        IEnumerable<T> GetByDescPrevPage(int pageSize, T param, string and = null, string returnFields = null);

        IEnumerable<T> GetByDescCurrentPage(int pageSize, T param, string and = null, string returnFields = null);

        IEnumerable<T> GetByDescNextPage(int pageSize, T param, string and = null, string returnFields = null);

        IEnumerable<T> GetByDescLastPage(int pageSize, object param = null, string and = null, string returnFields = null);
    }
}
