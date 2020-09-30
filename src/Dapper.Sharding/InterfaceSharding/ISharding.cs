using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Dapper.Sharding
{
    public abstract class ISharding<T> where T : class
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

        #region method curd

        public bool Delete(object id)
        {
            return GetTableById(id).Delete(id);
        }

        public int DeleteByIds(object ids)
        {
            if (CommonUtil.ObjectIsEmpty(ids))
                return 0;
            var idsList = CommonUtil.GetMultiExec(ids);
            var tran = BeginTran();
            try
            {
                int count = 0;
                foreach (var id in idsList)
                {
                    count += tran.GetTable(id).Delete(id) ? 1 : 0;
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

        public bool Exists(object id)
        {
            return GetTableById(id).Exists(id);
        }

        public T GetById(object id, string returnFields = null)
        {
            return GetTableById(id).GetById(id, returnFields);
        }

        public IEnumerable<T> GetByIds(object ids, string returnFields = null)
        {
            if (CommonUtil.ObjectIsEmpty(ids))
                return Enumerable.Empty<T>();
            var dict = CreateTableDictByIds(ids);

            var taskList = dict.Select(s =>
            {
                return Task.Run(() =>
                {
                    return s.Key.GetByIds(s.Value, returnFields);
                });
            });
            var result = Task.WhenAll(taskList).Result;
            return result.ConcatItem();
        }

        public bool Insert(T model)
        {
            return GetTableByModelAndInitId(model).Insert(model);
        }

        public bool InsertIfExistsUpdate(T model, string fields = null)
        {
            return GetTableByModel(model).InsertIfExistsUpdate(model, fields);
        }

        public bool InsertIfNoExists(T model)
        {
            return GetTableByModel(model).InsertIdentityIfNoExists(model);
        }

        public int InsertMany(IEnumerable<T> modelList)
        {
            var tran = BeginTran();
            try
            {
                int count = 0;
                foreach (var model in modelList)
                {
                    count += tran.GetTable(model).Insert(model) ? 1 : 0;
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

        public bool Update(T model)
        {
            return GetTableByModel(model).Update(model);
        }

        public bool UpdateExclude(T model, string fields)
        {
            return GetTableByModel(model).UpdateExclude(model, fields);
        }

        public int UpdateExcludeMany(IEnumerable<T> modelList, string fields)
        {
            var tran = BeginTran();
            try
            {
                int count = 0;
                foreach (var model in modelList)
                {
                    count += tran.GetTable(model).UpdateExclude(model, fields) ? 1 : 0;
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

        public bool UpdateInclude(T model, string fields)
        {
            return GetTableByModel(model).UpdateInclude(model, fields);
        }

        public int UpdateIncludeMany(IEnumerable<T> modelList, string fields)
        {
            var tran = BeginTran();
            try
            {
                int count = 0;
                foreach (var model in modelList)
                {
                    count += tran.GetTable(model).UpdateInclude(model, fields) ? 1 : 0;
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

        public int UpdateMany(IEnumerable<T> modelList)
        {
            var tran = BeginTran();
            try
            {
                int count = 0;
                foreach (var model in modelList)
                {
                    count += tran.GetTable(model).Update(model) ? 1 : 0;
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

        #endregion

        #region method abstract

        public abstract ITable<T> GetTableById(object id);

        public abstract ITable<T> GetTableByModel(T model);

        public abstract ITable<T> GetTableByModelAndInitId(T model);

        #endregion

    }
}
