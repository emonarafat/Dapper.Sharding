using System;
using System.Collections.Generic;
using System.Linq;
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

        public int UpdateByWhere(T model, string where, List<string> fields)
        {
            return Wrap(tran =>
            {
                int count = 0;
                foreach (var item in TableList)
                {
                    var tb = tran.GetTranTable(item);
                    count += tb.UpdateByWhere(model, where, fields);
                }
                return count;
            });

        }

        public int UpdateByWhereIgnore(T model, string where, List<string> fields)
        {
            return Wrap(tran =>
            {
                int count = 0;
                foreach (var item in TableList)
                {
                    var tb = tran.GetTranTable(item);
                    count += tb.UpdateByWhereIgnore(model, where, fields);
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

        public bool Insert(T model)
        {
            return GetTableByModel(model).Insert(model);
        }

        public void Insert(IEnumerable<T> modelList)
        {
            Wrap(tran =>
            {
                var dict = GetTableByGroupModelList(modelList);
                foreach (var item in dict)
                {
                    var tb = tran.GetTranTable(item.Key);
                    tb.Insert(item.Value);
                }
            });
        }

        public void InsertIfNoExists(T model)
        {
            GetTableByModel(model).InsertIfNoExists(model);
        }

        public void InsertIfNoExists(IEnumerable<T> modelList)
        {
            Wrap(tran =>
            {
                var dict = GetTableByGroupModelList(modelList);
                foreach (var item in dict)
                {
                    var tb = tran.GetTranTable(item.Key);
                    tb.InsertIfNoExists(item.Value);
                }
            });
        }

        public bool InsertIdentity(T model)
        {
            return GetTableByModel(model).InsertIdentity(model);
        }

        public void InsertIdentity(IEnumerable<T> modelList)
        {
            Wrap(tran =>
            {
                var dict = GetTableByGroupModelList(modelList);
                foreach (var item in dict)
                {
                    var tb = tran.GetTranTable(item.Key);
                    tb.InsertIdentity(item.Value);
                }
            });
        }

        public bool Update(T model)
        {
            return GetTableByModel(model).Update(model);
        }

        public void Update(IEnumerable<T> modelList)
        {
            Wrap(tran =>
            {
                var dict = GetTableByGroupModelList(modelList);
                foreach (var item in dict)
                {
                    var tb = tran.GetTranTable(item.Key);
                    tb.Update(item.Value);
                }
            });
        }

        public bool Update(T model, List<string> fields)
        {
            return GetTableByModel(model).Update(model, fields);
        }

        public void Update(IEnumerable<T> modelList, List<string> fields)
        {
            Wrap(tran =>
            {
                var dict = GetTableByGroupModelList(modelList);
                foreach (var item in dict)
                {
                    var tb = tran.GetTranTable(item.Key);
                    tb.Update(item.Value, fields);
                }
            });
        }

        public bool UpdateIgnore(T model, List<string> fields)
        {
            return GetTableByModel(model).UpdateIgnore(model, fields);
        }

        public void UpdateIgnore(IEnumerable<T> modelList, List<string> fields)
        {
            Wrap(tran =>
            {
                var dict = GetTableByGroupModelList(modelList);
                foreach (var item in dict)
                {
                    var tb = tran.GetTranTable(item.Key);
                    tb.UpdateIgnore(item.Value, fields);
                }
            });
        }

        public void Merge(T model, List<string> fields)
        {
            GetTableByModel(model).Merge(model, fields);
        }

        public void Merge(IEnumerable<T> modelList, List<string> fields)
        {
            Wrap(tran =>
            {
                var dict = GetTableByGroupModelList(modelList);
                foreach (var item in dict)
                {
                    var tb = tran.GetTranTable(item.Key);
                    tb.Merge(item.Value, fields);
                }
            });
        }

        public void MergeIgnore(T model, List<string> fields)
        {
            GetTableByModel(model).MergeIgnore(model, fields);
        }

        public void MergeIgnore(IEnumerable<T> modelList, List<string> fields)
        {
            Wrap(tran =>
            {
                var dict = GetTableByGroupModelList(modelList);
                foreach (var item in dict)
                {
                    var tb = tran.GetTranTable(item.Key);
                    tb.MergeIgnore(item.Value, fields);
                }
            });
        }

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

        #endregion

        #region method abstract

        public abstract ITable<T> GetTableById(object id);

        public abstract ITable<T> GetTableByModel(T model);

        public abstract ISharding<T> CreateTranSharding(DistributedTransaction tran);

        #endregion

    }
}
