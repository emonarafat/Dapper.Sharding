using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading.Tasks;

namespace Dapper.Sharding
{
    public abstract class ISharding<T> where T : class
    {
        public ISharding(ITable<T>[] list, DistributedTransaction tran = null)
        {
            if (list[0].SqlField.IsIdentity)
            {
                throw new Exception("auto increment of primary key is not allowed");
            }
            TableList = list;
            KeyName = TableList[0].SqlField.PrimaryKey;
            KeyType = TableList[0].SqlField.PrimaryKeyType;
            Query = new ShardingQuery<T>(TableList);
            DistributedTran = tran;
        }

        #region base

        public string KeyName { get; }

        public Type KeyType { get; }

        public ITable<T>[] TableList { get; }

        public ShardingQuery<T> Query { get; }

        private DistributedTransaction DistributedTran { get; }

        private void Wrap(Action<DistributedTransaction> action)
        {
            if (DistributedTran == null)
            {
                var tran = new DistributedTransaction();
                try
                {
                    action(tran);
                    tran.Commit();
                }
                catch (Exception ex)
                {
                    tran.Rollback();
                    throw ex;
                }

            }
            else
            {
                action(DistributedTran);
            }
        }

        private TResult Wrap<TResult>(Func<DistributedTransaction, TResult> func)
        {
            if (DistributedTran == null)
            {
                var tran = new DistributedTransaction();
                TResult result;
                try
                {
                    result = func(tran);
                    tran.Commit();
                }
                catch (Exception ex)
                {
                    tran.Rollback();
                    throw ex;
                }
                return result;
            }
            return func(DistributedTran);
        }

        #endregion

        #region method common

        public Dictionary<ITable<T>, List<object>> GetTableByGroupIds(object ids)
        {
            var dict = new Dictionary<ITable<T>, List<object>>();
            var idsList = CommonUtil.GetMultiExec(ids);
            if (idsList != null)
            {
                foreach (var id in idsList)
                {
                    var table = GetTableById(id);
                    if (!dict.ContainsKey(table))
                    {
                        dict.Add(table, new List<object>());
                    }
                    dict[table].Add(id);
                }
            }
            return dict;
        }

        public Dictionary<ITable<T>, List<T>> GetTableByGroupModelList(IEnumerable<T> modelList)
        {
            var dict = new Dictionary<ITable<T>, List<T>>();
            foreach (var item in modelList)
            {
                var table = GetTableByModel(item);
                if (!dict.ContainsKey(table))
                {
                    dict.Add(table, new List<T>());
                }
                dict[table].Add(item);
            }
            return dict;
        }

        public int UpdateByWhere(T model, string where)
        {
            return Wrap(tran =>
            {
                int count = 0;
                foreach (var item in TableList)
                {
                    var tb = tran.GetTranTable(item);
                    count += tb.UpdateByWhere(model, where);
                }
                return count;
            });
        }

        public int UpdateByWhereInclude(T model, string where, string fields)
        {
            return Wrap(tran =>
            {
                int count = 0;
                foreach (var item in TableList)
                {
                    var tb = tran.GetTranTable(item);
                    count += tb.UpdateByWhereInclude(model, where, fields);
                }
                return count;
            });

        }

        public int UpdateByWhereExclude(T model, string where, string fields)
        {
            return Wrap(tran =>
            {
                int count = 0;
                foreach (var item in TableList)
                {
                    var tb = tran.GetTranTable(item);
                    count += tb.UpdateByWhereExclude(model, where, fields);
                }
                return count;
            });
        }

        public int DeleteByWhere(string where, object param = null)
        {
            return Wrap(tran =>
            {
                int count = 0;
                foreach (var item in TableList)
                {
                    var tb = tran.GetTranTable(item);
                    count += tb.DeleteByWhere(where, param);
                }
                return count;
            });
        }

        public int DeleteAll()
        {
            return Wrap(tran =>
            {
                int count = 0;
                foreach (var item in TableList)
                {
                    var tb = tran.GetTranTable(item);
                    count += tb.DeleteAll();
                }
                return count;
            });
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
            return Wrap(tran =>
            {
                int count = 0;
                var dict = GetTableByGroupIds(ids);
                foreach (var item in dict)
                {
                    var tb = tran.GetTranTable(item.Key);
                    count += tb.DeleteByIds(item.Value);
                }
                return count;
            });
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
            var dict = GetTableByGroupIds(ids);
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
            return GetTableByModel(model).Insert(model);
        }

        public bool InsertIfExistsUpdate(T model, string fields = null)
        {
            return GetTableByModel(model).InsertIfExistsUpdate(model, fields);
        }

        public bool InsertIfNoExists(T model)
        {
            return GetTableByModel(model).InsertIdentityIfNoExists(model);
        }

        public void BulkInsert(IEnumerable<T> modelList)
        {
            Wrap(tran =>
            {
                var dict = GetTableByGroupModelList(modelList);
                foreach (var item in dict)
                {
                    var tb = tran.GetTranTable(item.Key);
                    tb.BulkInsert(item.Value);
                }
            });
        }

        //public bool Update(T model)
        //{
        //    return GetTableByModel(model).Update(model);
        //}

        //public bool UpdateExclude(T model, string fields)
        //{
        //    return GetTableByModel(model).UpdateExclude(model, fields);
        //}

        //public int UpdateExcludeMany(IEnumerable<T> modelList, string fields)
        //{
        //    var tran = BeginTran();
        //    try
        //    {
        //        int count = 0;
        //        foreach (var model in modelList)
        //        {
        //            count += tran.GetTable(model).UpdateExclude(model, fields) ? 1 : 0;
        //        }
        //        tran.Commit();
        //        return count;
        //    }
        //    catch (Exception ex)
        //    {
        //        tran.Rollback();
        //        throw ex;
        //    }
        //}

        //public bool UpdateInclude(T model, string fields)
        //{
        //    return GetTableByModel(model).UpdateInclude(model, fields);
        //}

        //public int UpdateIncludeMany(IEnumerable<T> modelList, string fields)
        //{
        //    var tran = BeginTran();
        //    try
        //    {
        //        int count = 0;
        //        foreach (var model in modelList)
        //        {
        //            count += tran.GetTable(model).UpdateInclude(model, fields) ? 1 : 0;
        //        }
        //        tran.Commit();
        //        return count;
        //    }
        //    catch (Exception ex)
        //    {
        //        tran.Rollback();
        //        throw ex;
        //    }
        //}

        //public int UpdateMany(IEnumerable<T> modelList)
        //{
        //    var tran = BeginTran();
        //    try
        //    {
        //        int count = 0;
        //        foreach (var model in modelList)
        //        {
        //            count += tran.GetTable(model).Update(model) ? 1 : 0;
        //        }
        //        tran.Commit();
        //        return count;
        //    }
        //    catch (Exception ex)
        //    {
        //        tran.Rollback();
        //        throw ex;
        //    }
        //}

        #endregion

        #region method abstract

        public abstract ITable<T> GetTableById(object id);

        public abstract ITable<T> GetTableByModel(T model);

        public abstract ISharding<T> CreateTranSharding(DistributedTransaction tran);

        #endregion

    }
}
