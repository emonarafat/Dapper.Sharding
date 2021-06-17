using FastMember;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Dapper.Sharding
{

    public abstract partial class ITable<T> where T : class
    {
        protected abstract string SqlInsert();

        protected abstract string SqlInsertIdentity();

        protected abstract string SqlUpdate(List<string> fields = null);

        protected abstract string SqlUpdateIgnore(List<string> fields);

        protected abstract string SqlUpdateByWhere(string where, List<string> fields = null);

        protected abstract string SqlUpdateByWhereIgnore(string where, List<string> fields);

        protected abstract string SqlDeleteById();

        protected abstract string SqlDeleteByIds();

        protected abstract string SqlDeleteByWhere(string where);

        protected abstract string SqlDeleteAll();
    }

    public abstract partial class ITable<T> where T : class
    {
        public ITable(string name, IDatabase database, SqlFieldEntity sqlField)
        {
            Name = name;
            DataBase = database;
            SqlField = sqlField;
        }

        #region prototype

        public string Name { get; }

        public IDatabase DataBase { get; }

        public DataBaseType DbType
        {
            get
            {
                return DataBase.Client.DbType;
            }
        }

        public SqlFieldEntity SqlField { get; }

        #endregion

        #region public

        public bool Exists(T model)
        {
            var accessor = TypeAccessor.Create(typeof(T));
            var id = accessor[model, SqlField.PrimaryKey];
            return Exists(id);
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

        public PageEntity<T> GetByPageAndCount(int page, int pageSize, string where = null, object param = null, string returnFields = null, string orderby = null)
        {
            return new PageEntity<T>
            {
                Data = GetByPage(page, pageSize, where, param, returnFields, orderby),
                Count = Count(where, param)
            };
        }

        public IEnumerable<dynamic> GetByPageDynamic(int page, int pageSize, string where = null, object param = null, string returnFields = null, string orderby = null)
        {
            int skip = 0;
            if (page > 0)
            {
                skip = (page - 1) * pageSize;
            }
            return GetBySkipTakeDynamic(skip, pageSize, where, param, returnFields, orderby);
        }

        public PageEntity<dynamic> GetByPageAndCountDynamic(int page, int pageSize, string where = null, object param = null, string returnFields = null, string orderby = null)
        {
            return new PageEntity<dynamic>
            {
                Data = GetByPageDynamic(page, pageSize, where, param, returnFields, orderby),
                Count = Count(where, param)
            };
        }

        public void Truncate()
        {
            DataBase.TruncateTable(Name);
        }

        public void Optimize(bool final = false, bool deduplicate = false)
        {
            DataBase.OptimizeTable(Name, final, deduplicate);
        }

        public void Optimize(string partition, bool final = false, bool deduplicate = false)
        {
            DataBase.OptimizeTable(Name, partition, final, deduplicate);
        }

        #endregion

        #region insert

        public void Insert(T model, DistributedTransaction tran = null, int? timeout = null)
        {
            var sql = SqlInsert();
            if (SqlField.IsIdentity)
            {
                var accessor = TypeAccessor.Create(typeof(T));
                if (SqlField.PrimaryKeyType == typeof(int))
                {
                    var id = DataBase.ExecuteScalar<int>(sql, model, tran, timeout);
                    accessor[model, SqlField.PrimaryKey] = id;
                }
                else
                {
                    var id = DataBase.ExecuteScalar<long>(sql, model, tran, timeout);
                    accessor[model, SqlField.PrimaryKey] = id;
                }
            }
            else
            {
                DataBase.Execute(sql, model, tran, timeout);
            }
        }

        public async Task InsertAsync(T model, DistributedTransaction tran = null, int? timeout = null)
        {
            var sql = SqlInsert();
            if (SqlField.IsIdentity)
            {
                var accessor = TypeAccessor.Create(typeof(T));
                if (SqlField.PrimaryKeyType == typeof(int))
                {
                    var id = await DataBase.ExecuteScalarAsync<int>(sql, model, tran, timeout);
                    accessor[model, SqlField.PrimaryKey] = id;
                }
                else
                {
                    var id = await DataBase.ExecuteScalarAsync<long>(sql, model, tran, timeout);
                    accessor[model, SqlField.PrimaryKey] = id;
                }
            }
            else
            {
                await DataBase.ExecuteAsync(sql, model, tran, timeout);
            }
        }

        public void InsertIdentity(T model, DistributedTransaction tran = null, int? timeout = null)
        {
            DataBase.Execute(SqlInsertIdentity(), model, tran, timeout);
        }

        public async Task InsertIdentityAsync(T model, DistributedTransaction tran = null, int? timeout = null)
        {
            await DataBase.ExecuteAsync(SqlInsertIdentity(), model, tran, timeout);
        }

        public virtual void Insert(T model, List<string> fields, DistributedTransaction tran = null, int? timeout = null)
        {
            DataBase.BulkInsert(Name, model, opt =>
            {
                opt.IgnoreOnInsertNames = SqlField.AllFieldExceptKeyList.Except(fields).ToList();
            }, tran);
        }

        public virtual void InsertIgnore(T model, List<string> fields, DistributedTransaction tran = null, int? timeout = null)
        {
            DataBase.BulkInsert(Name, model, opt =>
            {
                opt.IgnoreOnInsertNames = fields;
            }, tran);
        }

        public virtual void Insert(IEnumerable<T> modelList, DistributedTransaction tran = null, int? timeout = null)
        {
            DataBase.BulkInsert(Name, modelList, opt => { }, tran);
        }

        public virtual void Insert(IEnumerable<T> modelList, List<string> fields, DistributedTransaction tran = null, int? timeout = null)
        {
            DataBase.BulkInsert(Name, modelList, opt =>
            {
                opt.IgnoreOnInsertNames = SqlField.AllFieldExceptKeyList.Except(fields).ToList();
            }, tran);
        }

        public virtual void InsertIgnore(IEnumerable<T> modelList, List<string> fields, DistributedTransaction tran = null, int? timeout = null)
        {
            DataBase.BulkInsert(Name, modelList, opt =>
            {
                opt.IgnoreOnInsertNames = fields;
            }, tran);
        }

        public virtual void InsertIfNoExists(T model, DistributedTransaction tran = null, int? timeout = null)
        {
            DataBase.BulkInsert(Name, model, opt =>
            {
                opt.InsertIfNotExists = true;
            }, tran);
        }

        public virtual void InsertIfNoExists(T model, List<string> fields, DistributedTransaction tran = null, int? timeout = null)
        {
            DataBase.BulkInsert(Name, model, opt =>
            {
                opt.IgnoreOnInsertNames = SqlField.AllFieldExceptKeyList.Except(fields).ToList();
                opt.InsertIfNotExists = true;
            }, tran);
        }

        public virtual void InsertIfNoExistsIgnore(T model, List<string> fields, DistributedTransaction tran = null, int? timeout = null)
        {
            DataBase.BulkInsert(Name, model, opt =>
            {
                opt.IgnoreOnInsertNames = fields;
                opt.InsertIfNotExists = true;
            }, tran);
        }

        public virtual void InsertIfNoExists(IEnumerable<T> modelList, DistributedTransaction tran = null, int? timeout = null)
        {
            DataBase.BulkInsert(Name, modelList, opt =>
            {
                opt.InsertIfNotExists = true;
            }, tran);
        }

        public virtual void InsertIfNoExists(IEnumerable<T> modelList, List<string> fields, DistributedTransaction tran = null, int? timeout = null)
        {
            DataBase.BulkInsert(Name, modelList, opt =>
            {
                opt.IgnoreOnInsertNames = SqlField.AllFieldExceptKeyList.Except(fields).ToList();
                opt.InsertIfNotExists = true;
            }, tran);
        }

        public virtual void InsertIfNoExistsIgnore(IEnumerable<T> modelList, List<string> fields, DistributedTransaction tran = null, int? timeout = null)
        {
            DataBase.BulkInsert(Name, modelList, opt =>
            {
                opt.IgnoreOnInsertNames = fields;
                opt.InsertIfNotExists = true;
            }, tran);
        }

        public virtual void InsertIdentity(T model, List<string> fields, DistributedTransaction tran = null, int? timeout = null)
        {
            DataBase.BulkInsert(Name, model, opt =>
            {
                opt.IgnoreOnInsertNames = SqlField.AllFieldExceptKeyList.Except(fields).ToList();
                opt.InsertKeepIdentity = true;
            }, tran);
        }

        public virtual void InsertIdentityIgnore(T model, List<string> fields, DistributedTransaction tran = null, int? timeout = null)
        {
            DataBase.BulkInsert(Name, model, opt =>
            {
                opt.IgnoreOnInsertNames = fields;
                opt.InsertKeepIdentity = true;
            }, tran);
        }

        public virtual void InsertIdentity(IEnumerable<T> modelList, DistributedTransaction tran = null, int? timeout = null)
        {
            DataBase.BulkInsert(Name, modelList, opt =>
            {
                opt.InsertKeepIdentity = true;
            }, tran);
        }

        public virtual void InsertIdentity(IEnumerable<T> modelList, List<string> fields, DistributedTransaction tran = null, int? timeout = null)
        {
            DataBase.BulkInsert(Name, modelList, opt =>
            {
                opt.IgnoreOnInsertNames = SqlField.AllFieldExceptKeyList.Except(fields).ToList();
                opt.InsertKeepIdentity = true;
            }, tran);
        }

        public virtual void InsertIdentityIgnore(IEnumerable<T> modelList, List<string> fields, DistributedTransaction tran = null, int? timeout = null)
        {
            DataBase.BulkInsert(Name, modelList, opt =>
            {
                opt.IgnoreOnInsertNames = fields;
                opt.InsertKeepIdentity = true;
            }, tran);
        }

        public virtual void InsertIdentityIfNoExists(T model, DistributedTransaction tran = null, int? timeout = null)
        {
            DataBase.BulkInsert(Name, model, opt =>
            {
                opt.InsertKeepIdentity = true;
                opt.InsertIfNotExists = true;
            }, tran);
        }

        public virtual void InsertIdentityIfNoExists(T model, List<string> fields, DistributedTransaction tran = null, int? timeout = null)
        {
            DataBase.BulkInsert(Name, model, opt =>
            {
                opt.IgnoreOnInsertNames = SqlField.AllFieldExceptKeyList.Except(fields).ToList();
                opt.InsertKeepIdentity = true;
                opt.InsertIfNotExists = true;
            }, tran);
        }

        public virtual void InsertIdentityIfNoExistsIgnore(T model, List<string> fields, DistributedTransaction tran = null, int? timeout = null)
        {
            DataBase.BulkInsert(Name, model, opt =>
            {
                opt.IgnoreOnInsertNames = fields;
                opt.InsertKeepIdentity = true;
                opt.InsertIfNotExists = true;
            }, tran);
        }

        public virtual void InsertIdentityIfNoExists(IEnumerable<T> modelList, DistributedTransaction tran = null, int? timeout = null)
        {
            DataBase.BulkInsert(Name, modelList, opt =>
            {
                opt.InsertKeepIdentity = true;
                opt.InsertIfNotExists = true;
            }, tran);
        }

        public virtual void InsertIdentityIfNoExists(IEnumerable<T> modelList, List<string> fields, DistributedTransaction tran = null, int? timeout = null)
        {
            DataBase.BulkInsert(Name, modelList, opt =>
            {
                opt.IgnoreOnInsertNames = SqlField.AllFieldExceptKeyList.Except(fields).ToList();
                opt.InsertKeepIdentity = true;
                opt.InsertIfNotExists = true;
            }, tran);
        }

        public virtual void InsertIdentityIfNoExistsIgnore(IEnumerable<T> modelList, List<string> fields, DistributedTransaction tran = null, int? timeout = null)
        {
            DataBase.BulkInsert(Name, modelList, opt =>
            {
                opt.IgnoreOnInsertNames = fields;
                opt.InsertKeepIdentity = true;
                opt.InsertIfNotExists = true;
            }, tran);
        }

        public virtual void Merge(T model, List<string> fields = null, DistributedTransaction tran = null, int? timeout = null)
        {
            DataBase.BulkMerge(Name, model, opt =>
            {
                if (fields != null)
                {
                    var ignoreFileds = SqlField.AllFieldExceptKeyList.Except(fields).ToList();
                    opt.IgnoreOnMergeUpdateNames = ignoreFileds;
                }
                opt.MergeKeepIdentity = true;
            }, tran);
        }

        public virtual void Merge(IEnumerable<T> modelList, List<string> fields = null, DistributedTransaction tran = null, int? timeout = null)
        {
            DataBase.BulkMerge(Name, modelList, opt =>
            {
                if (fields != null)
                {
                    var ignoreFileds = SqlField.AllFieldExceptKeyList.Except(fields).ToList();
                    opt.IgnoreOnMergeUpdateNames = ignoreFileds;
                }
                opt.MergeKeepIdentity = true;
            }, tran);
        }

        public virtual void MergeIgnore(T model, List<string> fields = null, DistributedTransaction tran = null, int? timeout = null)
        {
            DataBase.BulkMerge(Name, model, opt =>
            {
                if (fields != null)
                {
                    opt.IgnoreOnMergeUpdateNames = fields;
                }
                opt.MergeKeepIdentity = true;
            }, tran);
        }

        public virtual void MergeIgnore(IEnumerable<T> modelList, List<string> fields = null, DistributedTransaction tran = null, int? timeout = null)
        {
            DataBase.BulkMerge(Name, modelList, opt =>
            {
                if (fields != null)
                {
                    opt.IgnoreOnMergeUpdateNames = fields;
                }
                opt.MergeKeepIdentity = true;
            }, tran);
        }

        #endregion

        #region update

        public int Update(T model, List<string> fields = null, DistributedTransaction tran = null, int? timeout = null)
        {
            return DataBase.Execute(SqlUpdate(fields), model, tran, timeout);
        }

        public Task<int> UpdateAsync(T model, List<string> fields = null, DistributedTransaction tran = null, int? timeout = null)
        {
            return DataBase.ExecuteAsync(SqlUpdate(fields), model, tran, timeout);
        }

        public int UpdateIgnore(T model, List<string> fields, DistributedTransaction tran = null, int? timeout = null)
        {
            return DataBase.Execute(SqlUpdateIgnore(fields), model, tran, timeout);
        }

        public Task<int> UpdateIgnoreAsync(T model, List<string> fields, DistributedTransaction tran = null, int? timeout = null)
        {
            return DataBase.ExecuteAsync(SqlUpdateIgnore(fields), model, tran, timeout);
        }

        public int UpdateByWhere(object model, string where, List<string> fields = null, DistributedTransaction tran = null, int? timeout = null)
        {
            return DataBase.Execute(SqlUpdateByWhere(where, fields), model, tran, timeout);
        }

        public Task<int> UpdateByWhereAsync(object model, string where, List<string> fields = null, DistributedTransaction tran = null, int? timeout = null)
        {
            return DataBase.ExecuteAsync(SqlUpdateByWhere(where, fields), model, tran, timeout);
        }

        public int UpdateByWhereIgnore(object model, string where, List<string> fields, DistributedTransaction tran = null, int? timeout = null)
        {
            return DataBase.Execute(SqlUpdateByWhereIgnore(where, fields), model, tran, timeout);
        }

        public Task<int> UpdateByWhereIgnoreAsync(object model, string where, List<string> fields, DistributedTransaction tran = null, int? timeout = null)
        {
            return DataBase.ExecuteAsync(SqlUpdateByWhereIgnore(where, fields), model, tran, timeout);
        }

        public virtual void Update(IEnumerable<T> modelList, List<string> fields = null, DistributedTransaction tran = null, int? timeout = null)
        {
            DataBase.BulkUpdate(Name, modelList, opt =>
            {
                if (fields != null)
                {
                    var ignoreFileds = SqlField.AllFieldExceptKeyList.Except(fields).ToList();
                    opt.IgnoreOnUpdateNames = ignoreFileds;
                }
            }, tran);
        }

        public virtual void UpdateIgnore(IEnumerable<T> modelList, List<string> fields, DistributedTransaction tran = null, int? timeout = null)
        {
            DataBase.BulkUpdate(Name, modelList, opt =>
            {
                opt.IgnoreOnUpdateNames = fields;
            }, tran);
        }

        #endregion

        #region delete

        public int Delete(object id, DistributedTransaction tran = null, int? timeout = null)
        {
            return DataBase.Execute(SqlDeleteById(), new { id }, tran, timeout);
        }

        public Task<int> DeleteAsync(object id, DistributedTransaction tran = null, int? timeout = null)
        {
            return DataBase.ExecuteAsync(SqlDeleteById(), new { id }, tran, timeout);
        }

        public int Delete(T model, DistributedTransaction tran = null, int? timeout = null)
        {
            var accessor = TypeAccessor.Create(typeof(T));
            var id = accessor[model, SqlField.PrimaryKey];
            return Delete(id, tran, timeout);
        }

        public Task<int> DeleteAsync(T model, DistributedTransaction tran = null, int? timeout = null)
        {
            var accessor = TypeAccessor.Create(typeof(T));
            var id = accessor[model, SqlField.PrimaryKey];
            return DeleteAsync(id, tran, timeout);
        }

        public int DeleteByIds(object ids, DistributedTransaction tran = null, int? timeout = null)
        {
            if (CommonUtil.ObjectIsEmpty(ids))
                return 0;
            var dpar = new DynamicParameters();
            if (DbType != DataBaseType.Oracle)
            {
                dpar.Add("@ids", ids);
            }
            else
            {
                dpar.Add(":ids", ids);
            }
            return DataBase.Execute(SqlDeleteByIds(), dpar, tran, timeout);
        }

        public Task<int> DeleteByIdsAsync(object ids, DistributedTransaction tran = null, int? timeout = null)
        {
            if (CommonUtil.ObjectIsEmpty(ids))
                return Task.FromResult(0);
            var dpar = new DynamicParameters();
            if (DbType != DataBaseType.Oracle)
            {
                dpar.Add("@ids", ids);
            }
            else
            {
                dpar.Add(":ids", ids);
            }
            return DataBase.ExecuteAsync(SqlDeleteByIds(), dpar, tran, timeout);
        }

        public int DeleteByWhere(string where, object param = null, DistributedTransaction tran = null, int? timeout = null)
        {
            return DataBase.Execute(SqlDeleteByWhere(where), param, tran, timeout);
        }

        public Task<int> DeleteByWhereAsync(string where, object param = null, DistributedTransaction tran = null, int? timeout = null)
        {
            return DataBase.ExecuteAsync(SqlDeleteByWhere(where), param, tran, timeout);
        }

        public int DeleteAll(DistributedTransaction tran = null, int? timeout = null)
        {
            var sql = SqlDeleteAll();
            if (sql == null)
            {
                return 1;
            }
            return DataBase.Execute(sql, null, tran, timeout);
        }

        public Task<int> DeleteAllAsync(DistributedTransaction tran = null, int? timeout = null)
        {
            var sql = SqlDeleteAll();
            if (sql == null)
            {
                return Task.FromResult(1);
            }
            return DataBase.ExecuteAsync(sql, null, tran, timeout);
        }

        public virtual void Delete(IEnumerable<T> modelList)
        {
            DataBase.BulkDelete(Name, modelList, opt => { });
        }

        #endregion
    }


    #region abstract

    public abstract partial class ITable<T> where T : class
    {
        public abstract bool Exists(object id);

        public abstract long Count(string where = null, object param = null);

        public abstract TValue Min<TValue>(string field, string where = null, object param = null);

        public abstract TValue Max<TValue>(string field, string where = null, object param = null);

        public abstract TValue Sum<TValue>(string field, string where = null, object param = null);

        public abstract decimal Avg(string field, string where = null, object param = null);
    }

    #endregion

    #region abstract query method

    public abstract partial class ITable<T> where T : class
    {
        public abstract IEnumerable<T> GetAll(string returnFields = null, string orderby = null);

        public abstract T GetById(object id, string returnFields = null);

        public abstract T GetByIdForUpdate(object id, string returnFields = null);

        public abstract IEnumerable<T> GetByIds(object ids, string returnFields = null);

        public abstract IEnumerable<T> GetByIdsForUpdate(object ids, string returnFields = null);

        public abstract IEnumerable<T> GetByIdsWithField(object ids, string field, string returnFields = null);

        public abstract IEnumerable<T> GetByWhere(string where, object param = null, string returnFields = null, string orderby = null, int limit = 0);

        public abstract T GetByWhereFirst(string where, object param = null, string returnFields = null);

        public abstract IEnumerable<T> GetBySkipTake(int skip, int take, string where = null, object param = null, string returnFields = null, string orderby = null);

        public abstract IEnumerable<T> GetByAscFirstPage(int pageSize, object param = null, string and = null, string returnFields = null);

        public abstract IEnumerable<T> GetByAscPrevPage(int pageSize, T param, string and = null, string returnFields = null);

        public abstract IEnumerable<T> GetByAscCurrentPage(int pageSize, T param, string and = null, string returnFields = null);

        public abstract IEnumerable<T> GetByAscNextPage(int pageSize, T param, string and = null, string returnFields = null);

        public abstract IEnumerable<T> GetByAscLastPage(int pageSize, object param = null, string and = null, string returnFields = null);

        public abstract IEnumerable<T> GetByDescFirstPage(int pageSize, object param = null, string and = null, string returnFields = null);

        public abstract IEnumerable<T> GetByDescPrevPage(int pageSize, T param, string and = null, string returnFields = null);

        public abstract IEnumerable<T> GetByDescCurrentPage(int pageSize, T param, string and = null, string returnFields = null);

        public abstract IEnumerable<T> GetByDescNextPage(int pageSize, T param, string and = null, string returnFields = null);

        public abstract IEnumerable<T> GetByDescLastPage(int pageSize, object param = null, string and = null, string returnFields = null);

    }

    #endregion

    #region abstract query method dynamic

    public abstract partial class ITable<T> where T : class
    {
        public abstract IEnumerable<dynamic> GetAllDynamic(string returnFields = null, string orderby = null);

        public abstract dynamic GetByIdDynamic(object id, string returnFields = null);

        public abstract dynamic GetByIdForUpdateDynamic(object id, string returnFields = null);

        public abstract IEnumerable<dynamic> GetByIdsDynamic(object ids, string returnFields = null);

        public abstract IEnumerable<dynamic> GetByIdsForUpdateDynamic(object ids, string returnFields = null);

        public abstract IEnumerable<dynamic> GetByIdsWithFieldDynamic(object ids, string field, string returnFields = null);

        public abstract IEnumerable<dynamic> GetByWhereDynamic(string where, object param = null, string returnFields = null, string orderby = null, int limit = 0);

        public abstract dynamic GetByWhereFirstDynamic(string where, object param = null, string returnFields = null);

        public abstract IEnumerable<dynamic> GetBySkipTakeDynamic(int skip, int take, string where = null, object param = null, string returnFields = null, string orderby = null);

        public abstract IEnumerable<dynamic> GetByAscFirstPageDynamic(int pageSize, object param = null, string and = null, string returnFields = null);

        public abstract IEnumerable<dynamic> GetByAscPrevPageDynamic(int pageSize, T param, string and = null, string returnFields = null);

        public abstract IEnumerable<dynamic> GetByAscCurrentPageDynamic(int pageSize, T param, string and = null, string returnFields = null);

        public abstract IEnumerable<dynamic> GetByAscNextPageDynamic(int pageSize, T param, string and = null, string returnFields = null);

        public abstract IEnumerable<dynamic> GetByAscLastPageDynamic(int pageSize, object param = null, string and = null, string returnFields = null);

        public abstract IEnumerable<dynamic> GetByDescFirstPageDynamic(int pageSize, object param = null, string and = null, string returnFields = null);

        public abstract IEnumerable<dynamic> GetByDescPrevPageDynamic(int pageSize, T param, string and = null, string returnFields = null);

        public abstract IEnumerable<dynamic> GetByDescCurrentPageDynamic(int pageSize, T param, string and = null, string returnFields = null);

        public abstract IEnumerable<dynamic> GetByDescNextPageDynamic(int pageSize, T param, string and = null, string returnFields = null);

        public abstract IEnumerable<dynamic> GetByDescLastPageDynamic(int pageSize, object param = null, string and = null, string returnFields = null);

    }

    #endregion
}
