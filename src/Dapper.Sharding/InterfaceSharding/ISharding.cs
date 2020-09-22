using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dapper.Sharding
{
    public abstract class ISharding<T>
    {
        public ISharding(params ITable<T>[] list)
        {
            if (list[0].SqlField.IsIdentity)
            {
                throw new Exception("auto increment of primary key is not allowed");
            }
            TableList = list;
            KeyName = list[0].SqlField.PrimaryKey;
            KeyType = list[0].SqlField.PrimaryKeyType;
        }

        public ITable<T>[] TableList { get; }

        #region method common

        public string KeyName { get; }

        public Type KeyType { get; }

        public ITable<T> GetTableById(string id)
        {
            return TableList[ShardingUtils.Mod(id, TableList.Length)];
        }

        public ITable<T> GetTableById(object id)
        {
            return TableList[ShardingUtils.Mod(id, TableList.Length)];
        }

        public ITable<T> GetTableByModel(T model)
        {
            return TableList[ShardingUtils.Mod(model, KeyName, KeyType, TableList.Length)];
        }

        public ITable<T> GetTableByModelAndInitId(T model)
        {
            return TableList[ShardingUtils.ModAndInitId(model, KeyName, KeyType, TableList.Length)];
        }

        public int UpdateByWhere(T model, string where)
        {
            return 0;
        }

        public int UpdateByWhereInclude(T model, string where, string fields)
        {
            return 0;
        }

        public int UpdateByWhereExclude(T model, string where, string fields)
        {
            return 0;
        }

        public int DeleteByWhere(string where, object param = null)
        {
            return 0;
        }

        public int DeleteAll()
        {
            return 0;
        }

        public long Count()
        {
            var taskList = new List<Task<long>>();
            foreach (var item in TableList)
            {
                var task = Task.Run(() =>
                {
                    return item.Count();
                });
                taskList.Add(task);
            }
            return Task.WhenAll(taskList).Result.Sum();
        }

        public long Count(string where, object param = null)
        {
            return 0;
        }

        public TValue Min<TValue>(string field, string where = null, object param = null)
        {
            return default;
        }

        public TValue Max<TValue>(string field, string where = null, object param = null)
        {
            return default;
        }

        public TValue Sum<TValue>(string field, string where = null, object param = null)
        {
            return default;
        }

        public TValue Avg<TValue>(string field, string where = null, object param = null)
        {
            return default;
        }

        public IEnumerable<T> GetAll(string returnFields = null, string orderBy = null)
        {
            return default;
        }

        public IEnumerable<T> GetByIdsWithField(object ids, string field, string returnFields = null)
        {
            return default;
        }

        public IEnumerable<T> GetByWhere(string where, object param = null, string returnFields = null)
        {
            return default;
        }

        public T GetByWhereFirst(string where, object param = null, string returnFields = null)
        {
            return default;
        }

        public IEnumerable<T> GetByAscFirstPage(int pageSize, object param = null, string and = null, string returnFields = null)
        {
            return default;
        }

        public IEnumerable<T> GetByAscPrevPage(int pageSize, T param, string and = null, string returnFields = null)
        {
            return default;
        }

        public IEnumerable<T> GetByAscCurrentPage(int pageSize, T param, string and = null, string returnFields = null)
        {
            return default;
        }

        public IEnumerable<T> GetByAscNextPage(int pageSize, T param, string and = null, string returnFields = null)
        {
            return default;
        }

        public IEnumerable<T> GetByAscLastPage(int pageSize, object param = null, string and = null, string returnFields = null)
        {
            return default;
        }

        public IEnumerable<T> GetByDescFirstPage(int pageSize, object param = null, string and = null, string returnFields = null)
        {
            return default;
        }

        public IEnumerable<T> GetByDescPrevPage(int pageSize, T param, string and = null, string returnFields = null)
        {
            return default;
        }

        public IEnumerable<T> GetByDescCurrentPage(int pageSize, T param, string and = null, string returnFields = null)
        {
            return default;
        }

        public IEnumerable<T> GetByDescNextPage(int pageSize, T param, string and = null, string returnFields = null)
        {
            return default;
        }

        public IEnumerable<T> GetByDescLastPage(int pageSize, object param = null, string and = null, string returnFields = null)
        {
            return default;
        }

        #endregion

        #region method abstract

        public abstract bool Insert(T model);

        public abstract int InsertMany(IEnumerable<T> modelList);

        public abstract bool InsertIfNoExists(T model);

        public abstract bool InsertIfExistsUpdate(T model, string fields = null);

        public abstract bool Update(T model);

        public abstract int UpdateMany(IEnumerable<T> modelList);

        public abstract bool UpdateInclude(T model, string fields);

        public abstract int UpdateIncludeMany(IEnumerable<T> modelList, string fields);

        public abstract bool UpdateExclude(T model, string fields);

        public abstract int UpdateExcludeMany(IEnumerable<T> modelList, string fields);

        public abstract bool Delete(object id);

        public abstract int DeleteByIds(object ids);

        public abstract bool Exists(object id);

        public abstract T GetById(object id, string returnFields = null);

        public abstract IEnumerable<T> GetByIds(object ids, string returnFields = null);

        #endregion

    }
}
