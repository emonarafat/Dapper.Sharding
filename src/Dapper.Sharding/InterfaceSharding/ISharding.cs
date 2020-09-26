using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Dapper.Sharding
{
    public abstract class ISharding<T>
    {
        public ISharding(ITable<T>[] list)
        {
            if (list[0].SqlField.IsIdentity)
            {
                throw new Exception("auto increment of primary key is not allowed");
            }
            TableList = list;
            KeyName = TableList[0].SqlField.PrimaryKey;
            KeyType = TableList[0].SqlField.PrimaryKeyType;
            Query = new ShardingQuery<T>(TableList);
        }

        #region base

        public string KeyName { get; }

        public Type KeyType { get; }

        public ITable<T>[] TableList { get; }

        public ShardingQuery<T> Query { get; }

        #endregion

        #region method common

        public ShardingTran<T> BeginTran()
        {
            return new ShardingTran<T>(this);
        }

        public Dictionary<ITable<T>, List<object>> CreateTableDictByIds(object ids)
        {
            var dict = new Dictionary<ITable<T>, List<object>>();
            var idsList = CommonUtil.GetMultiExec(ids);
            foreach (var id in idsList)
            {
                var table = GetTableById(id);
                if (!dict.ContainsKey(table))
                {
                    dict.Add(table, new List<object>());
                }
                dict[table].Add(id);
            }
            return dict;
        }

        public int UpdateByWhere(T model, string where)
        {
            var tran = BeginTran();
            try
            {
                int count = 0;
                var tables = tran.GetTableList();
                foreach (var tb in tables)
                {
                    count += tb.UpdateByWhere(model, where);
                }
                tran.Commit();
                return count;
            }
            catch (Exception ex)
            {
                tran.Rollback();
                throw ex;
            }
        }

        public int UpdateByWhereInclude(T model, string where, string fields)
        {
            var tran = BeginTran();
            try
            {
                int count = 0;
                var tables = tran.GetTableList();
                foreach (var tb in tables)
                {
                    count += tb.UpdateByWhereInclude(model, where, fields);
                }
                tran.Commit();
                return count;
            }
            catch (Exception ex)
            {
                tran.Rollback();
                throw ex;
            }
        }

        public int UpdateByWhereExclude(T model, string where, string fields)
        {
            var tran = BeginTran();
            try
            {
                int count = 0;
                var tables = tran.GetTableList();
                foreach (var tb in tables)
                {
                    count += tb.UpdateByWhereExclude(model, where, fields);
                }
                tran.Commit();
                return count;
            }
            catch (Exception ex)
            {
                tran.Rollback();
                throw ex;
            }
        }

        public int DeleteByWhere(string where, object param = null)
        {
            var tran = BeginTran();
            try
            {
                int count = 0;
                var tables = tran.GetTableList();
                foreach (var tb in tables)
                {
                    count += tb.DeleteByWhere(where, param);
                }
                tran.Commit();
                return count;
            }
            catch (Exception ex)
            {
                tran.Rollback();
                throw ex;
            }
        }

        public int DeleteAll()
        {
            var tran = BeginTran();
            try
            {
                int count = 0;
                var tables = tran.GetTableList();
                foreach (var tb in tables)
                {
                    count += tb.DeleteAll();
                }
                tran.Commit();
                return count;
            }
            catch (Exception ex)
            {
                tran.Rollback();
                throw ex;
            }
        }

        public void Truncate()
        {
            var taskList = TableList.Select(s =>
            {
                return Task.Run(() =>
                {
                    s.Truncate();
                });
            });
            Task.WhenAll(taskList).Wait();
        }

        #endregion

        #region method abstract

        public abstract ITable<T> GetTableById(string id);

        public abstract ITable<T> GetTableById(object id);

        public abstract ITable<T> GetTableByModel(T model);

        public abstract ITable<T> GetTableByModelAndInitId(T model);

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
