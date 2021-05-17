using FastMember;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

namespace Dapper.Sharding
{
    public abstract class ITable<T> where T : class
    {
        public ITable(string name, IDatabase database, SqlFieldEntity sqlField, DapperEntity dpEntity)
        {
            Name = name;
            DataBase = database;
            SqlField = sqlField;
            DpEntity = dpEntity;
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

        public DapperEntity DpEntity { get; }

        #endregion

        #region public virtual

        public virtual bool Insert(T model)
        {
            DpEntity.BulkInsert(model, opt => { });
            return true;
        }

        public virtual bool Insert(T model, List<string> fields)
        {
            DpEntity.BulkInsert(model, opt =>
            {
                opt.IgnoreOnInsertNames = SqlField.AllFieldExceptKeyList.Except(fields).ToList();
            });
            return true;
        }

        public virtual bool InsertIgnore(T model, List<string> fields)
        {
            DpEntity.BulkInsert(model, opt =>
            {
                opt.IgnoreOnInsertNames = fields;
            });
            return true;
        }

        public virtual void Insert(IEnumerable<T> modelList)
        {
            DpEntity.BulkInsert(modelList, opt => { });
        }

        public virtual bool Insert(IEnumerable<T> modelList, List<string> fields)
        {
            DpEntity.BulkInsert(modelList, opt =>
            {
                opt.IgnoreOnInsertNames = SqlField.AllFieldExceptKeyList.Except(fields).ToList();
            });
            return true;
        }

        public virtual bool InsertIgnore(IEnumerable<T> modelList, List<string> fields)
        {
            DpEntity.BulkInsert(modelList, opt =>
            {
                opt.IgnoreOnInsertNames = fields;
            });
            return true;
        }

        public virtual void InsertIfNoExists(T model)
        {
            DpEntity.BulkInsert(model, opt =>
            {
                opt.InsertIfNotExists = true;
            });
        }

        public virtual void InsertIfNoExists(T model, List<string> fields)
        {
            DpEntity.BulkInsert(model, opt =>
            {
                opt.IgnoreOnInsertNames = SqlField.AllFieldExceptKeyList.Except(fields).ToList();
                opt.InsertIfNotExists = true;
            });
        }

        public virtual void InsertIfNoExistsIgnore(T model, List<string> fields)
        {
            DpEntity.BulkInsert(model, opt =>
            {
                opt.IgnoreOnInsertNames = fields;
                opt.InsertIfNotExists = true;
            });
        }

        public virtual void InsertIfNoExists(IEnumerable<T> modelList)
        {
            DpEntity.BulkInsert(modelList, opt =>
            {
                opt.InsertIfNotExists = true;
            });
        }

        public virtual void InsertIfNoExists(IEnumerable<T> modelList, List<string> fields)
        {
            DpEntity.BulkInsert(modelList, opt =>
            {
                opt.IgnoreOnInsertNames = SqlField.AllFieldExceptKeyList.Except(fields).ToList();
                opt.InsertIfNotExists = true;
            });
        }

        public virtual void InsertIfNoExistsIgnore(IEnumerable<T> modelList, List<string> fields)
        {
            DpEntity.BulkInsert(modelList, opt =>
            {
                opt.IgnoreOnInsertNames = fields;
                opt.InsertIfNotExists = true;
            });
        }

        public virtual bool InsertIdentity(T model)
        {
            DpEntity.BulkInsert(model, opt =>
            {
                opt.InsertKeepIdentity = true;
            });
            return true;
        }

        public virtual bool InsertIdentity(T model, List<string> fields)
        {
            DpEntity.BulkInsert(model, opt =>
            {
                opt.IgnoreOnInsertNames = SqlField.AllFieldExceptKeyList.Except(fields).ToList();
                opt.InsertKeepIdentity = true;
            });
            return true;
        }

        public virtual bool InsertIdentityIgnore(T model, List<string> fields)
        {
            DpEntity.BulkInsert(model, opt =>
            {
                opt.IgnoreOnInsertNames = fields;
                opt.InsertKeepIdentity = true;
            });
            return true;
        }

        public virtual void InsertIdentity(IEnumerable<T> modelList)
        {
            DpEntity.BulkInsert(modelList, opt =>
            {
                opt.InsertKeepIdentity = true;
            });
        }

        public virtual void InsertIdentity(IEnumerable<T> modelList, List<string> fields)
        {
            DpEntity.BulkInsert(modelList, opt =>
            {
                opt.IgnoreOnInsertNames = SqlField.AllFieldExceptKeyList.Except(fields).ToList();
                opt.InsertKeepIdentity = true;
            });
        }

        public virtual void InsertIdentityIgnore(IEnumerable<T> modelList, List<string> fields)
        {
            DpEntity.BulkInsert(modelList, opt =>
            {
                opt.IgnoreOnInsertNames = fields;
                opt.InsertKeepIdentity = true;
            });
        }

        public virtual void InsertIdentityIfNoExists(T model)
        {
            DpEntity.BulkInsert(model, opt =>
            {
                opt.InsertKeepIdentity = true;
                opt.InsertIfNotExists = true;
            });
        }

        public virtual void InsertIdentityIfNoExists(T model, List<string> fields)
        {
            DpEntity.BulkInsert(model, opt =>
            {
                opt.IgnoreOnInsertNames = SqlField.AllFieldExceptKeyList.Except(fields).ToList();
                opt.InsertKeepIdentity = true;
                opt.InsertIfNotExists = true;
            });
        }

        public virtual void InsertIdentityIfNoExistsIgnore(T model, List<string> fields)
        {
            DpEntity.BulkInsert(model, opt =>
            {
                opt.IgnoreOnInsertNames = fields;
                opt.InsertKeepIdentity = true;
                opt.InsertIfNotExists = true;
            });
        }

        public virtual void InsertIdentityIfNoExists(IEnumerable<T> modelList)
        {
            DpEntity.BulkInsert(modelList, opt =>
            {
                opt.InsertKeepIdentity = true;
                opt.InsertIfNotExists = true;
            });
        }

        public virtual void InsertIdentityIfNoExists(IEnumerable<T> modelList, List<string> fields)
        {
            DpEntity.BulkInsert(modelList, opt =>
            {
                opt.IgnoreOnInsertNames = SqlField.AllFieldExceptKeyList.Except(fields).ToList();
                opt.InsertKeepIdentity = true;
                opt.InsertIfNotExists = true;
            });
        }

        public virtual void InsertIdentityIfNoExistsIgnore(IEnumerable<T> modelList, List<string> fields)
        {
            DpEntity.BulkInsert(modelList, opt =>
            {
                opt.IgnoreOnInsertNames = fields;
                opt.InsertKeepIdentity = true;
                opt.InsertIfNotExists = true;
            });
        }

        public virtual bool Update(T model, List<string> fields = null)
        {
            DpEntity.BulkUpdate(model, opt =>
            {
                if (fields != null)
                {
                    var ignoreFileds = SqlField.AllFieldExceptKeyList.Except(fields).ToList();
                    opt.IgnoreOnUpdateNames = ignoreFileds;
                }
            });
            return true;
        }

        public virtual void Update(IEnumerable<T> modelList, List<string> fields = null)
        {
            DpEntity.BulkUpdate(modelList, opt =>
            {
                if (fields != null)
                {
                    var ignoreFileds = SqlField.AllFieldExceptKeyList.Except(fields).ToList();
                    opt.IgnoreOnUpdateNames = ignoreFileds;
                }
            });
        }

        public virtual bool UpdateIgnore(T model, List<string> fields)
        {
            DpEntity.BulkUpdate(model, opt =>
            {
                opt.IgnoreOnUpdateNames = fields;
            });
            return true;
        }

        public virtual void UpdateIgnore(IEnumerable<T> modelList, List<string> fields)
        {
            DpEntity.BulkUpdate(modelList, opt =>
            {
                opt.IgnoreOnUpdateNames = fields;
            });
        }

        public virtual void Delete(T model)
        {
            DpEntity.BulkDelete(model, opt => { });
        }

        public virtual void Delete(IEnumerable<T> modelList)
        {
            DpEntity.BulkDelete(modelList, opt => { });
        }

        public virtual void Merge(T model, List<string> fields = null)
        {
            DpEntity.BulkMerge(model, opt =>
            {
                if (fields != null)
                {
                    var ignoreFileds = SqlField.AllFieldExceptKeyList.Except(fields).ToList();
                    opt.IgnoreOnMergeUpdateNames = ignoreFileds;
                }
                opt.MergeKeepIdentity = true;
            });
        }

        public virtual void Merge(IEnumerable<T> modelList, List<string> fields = null)
        {
            DpEntity.BulkMerge(modelList, opt =>
            {
                if (fields != null)
                {
                    var ignoreFileds = SqlField.AllFieldExceptKeyList.Except(fields).ToList();
                    opt.IgnoreOnMergeUpdateNames = ignoreFileds;
                }
                opt.MergeKeepIdentity = true;
            });
        }

        public virtual void MergeIgnore(T model, List<string> fields = null)
        {
            DpEntity.BulkMerge(model, opt =>
            {
                if (fields != null)
                {
                    opt.IgnoreOnMergeUpdateNames = fields;
                }
                opt.MergeKeepIdentity = true;
            });
        }

        public virtual void MergeIgnore(IEnumerable<T> modelList, List<string> fields = null)
        {
            DpEntity.BulkMerge(modelList, opt =>
            {
                if (fields != null)
                {
                    opt.IgnoreOnMergeUpdateNames = fields;
                }
                opt.MergeKeepIdentity = true;
            });
        }

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

        #region abstract method

        public abstract ITable<T> CreateTranTable(IDbConnection conn, IDbTransaction tran, int? commandTimeout = null);

        public abstract int UpdateByWhere(T model, string where, List<string> fields = null);

        public abstract int UpdateByWhereIgnore(T model, string where, List<string> fields);

        public abstract int UpdateByWhere(string where, object param, List<string> fields = null);

        public abstract bool Delete(object id);

        public abstract int DeleteByIds(object ids);

        public abstract int DeleteByWhere(string where, object param = null);

        public abstract int DeleteAll();

        //public abstract void Truncate();

        public abstract bool Exists(object id);

        public abstract long Count(string where = null, object param = null);

        public abstract TValue Min<TValue>(string field, string where = null, object param = null);

        public abstract TValue Max<TValue>(string field, string where = null, object param = null);

        public abstract TValue Sum<TValue>(string field, string where = null, object param = null);

        public abstract decimal Avg(string field, string where = null, object param = null);

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


        #endregion

        #region abstract method dynamic

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

        #endregion
    }
}
