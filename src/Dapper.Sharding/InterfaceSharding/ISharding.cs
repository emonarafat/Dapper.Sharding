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

        protected void Wrap(Action<DistributedTransaction> action)
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

        protected TResult Wrap<TResult>(Func<DistributedTransaction, TResult> func)
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

        public virtual Dictionary<ITable<T>, List<object>> GetTableByGroupIds(object ids)
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

        public virtual Dictionary<ITable<T>, List<T>> GetTableByGroupModelList(IEnumerable<T> modelList)
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

        public int UpdateByWhere(T model, string where, List<string> fields = null)
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

        public virtual bool Insert(T model)
        {
            return Wrap(tran =>
            {
                var tb = tran.GetTranTable(GetTableByModel(model));
                return tb.Insert(model);
            });
        }

        public virtual void Insert(IEnumerable<T> modelList)
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

        public virtual void InsertIfNoExists(T model)
        {
            Wrap(tran =>
            {
                var tb = tran.GetTranTable(GetTableByModel(model));
                tb.InsertIfNoExists(model);
            });
        }

        public virtual void InsertIfNoExists(IEnumerable<T> modelList)
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

        public virtual bool InsertIdentity(T model)
        {
            return Wrap(tran =>
            {
                var tb = tran.GetTranTable(GetTableByModel(model));
                return tb.InsertIdentity(model);
            });
        }

        public virtual void InsertIdentity(IEnumerable<T> modelList)
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

        public virtual void InsertIdentityIfNoExists(T model)
        {
            Wrap(tran =>
            {
                var tb = tran.GetTranTable(GetTableByModel(model));
                tb.InsertIdentityIfNoExists(model);
            });
        }

        public virtual void InsertIdentityIfNoExists(IEnumerable<T> modelList)
        {
            Wrap(tran =>
            {
                var dict = GetTableByGroupModelList(modelList);
                foreach (var item in dict)
                {
                    var tb = tran.GetTranTable(item.Key);
                    tb.InsertIdentityIfNoExists(item.Value);
                }
            });
        }

        public virtual bool Update(T model, List<string> fields = null)
        {
            return Wrap(tran =>
            {
                var tb = tran.GetTranTable(GetTableByModel(model));
                return tb.Update(model, fields);
            });
        }

        public virtual void Update(IEnumerable<T> modelList, List<string> fields = null)
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

        public virtual bool UpdateIgnore(T model, List<string> fields)
        {
            return Wrap(tran =>
            {
                var tb = tran.GetTranTable(GetTableByModel(model));
                return tb.UpdateIgnore(model, fields);
            });
        }

        public virtual void UpdateIgnore(IEnumerable<T> modelList, List<string> fields)
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

        public virtual void Merge(T model, List<string> fields = null)
        {
            Wrap(tran =>
           {
               var tb = tran.GetTranTable(GetTableByModel(model));
               tb.Merge(model, fields);
           });
        }

        public virtual void Merge(IEnumerable<T> modelList, List<string> fields = null)
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

        public virtual void MergeIgnore(T model, List<string> fields)
        {
            Wrap(tran =>
            {
                var tb = tran.GetTranTable(GetTableByModel(model));
                tb.MergeIgnore(model, fields);
            });
        }

        public virtual void MergeIgnore(IEnumerable<T> modelList, List<string> fields)
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

        public virtual bool Delete(object id)
        {
            return Wrap(tran =>
            {
                var tb = tran.GetTranTable(GetTableById(id));
                return tb.Delete(id);
            });
        }

        public virtual int DeleteByIds(object ids)
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

        public virtual bool Exists(object id)
        {
            return GetTableById(id).Exists(id);
        }

        public virtual bool Exists(T model)
        {
            return GetTableByModel(model).Exists(model);
        }

        public virtual T GetById(object id, string returnFields = null)
        {
            return GetTableById(id).GetById(id, returnFields);
        }

        public virtual IEnumerable<T> GetByIds(object ids, string returnFields = null)
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
