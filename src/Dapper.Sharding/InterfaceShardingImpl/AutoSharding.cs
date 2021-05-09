using FastMember;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dapper.Sharding
{
    public class AutoSharding<T> : ISharding<T> where T : class
    {
        public AutoSharding(ITable<T>[] tableList, DistributedTransaction tran = null) : base(tableList, tran)
        {

        }

        private static readonly string _msg = "AutoSharding not support";

        #region base

        public ITable<T> _GetTableById(object id)
        {
            return TableList[ShardingUtils.Mod(id, TableList.Length)];
        }

        public ITable<T> _GetTableByModel(T model)
        {
            return TableList[ShardingUtils.Mod(model, SqlField.PrimaryKey, SqlField.PrimaryKeyType, TableList.Length)];
        }

        public Dictionary<ITable<T>, List<object>> _GetTableByGroupIds(object ids)
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

        public Dictionary<ITable<T>, List<T>> _GetTableByGroupModelList(IEnumerable<T> modelList)
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

        #endregion

        #region virtual

        public override Dictionary<ITable<T>, List<object>> GetTableByGroupIds(object ids)
        {
            throw new Exception(_msg);
        }

        public override Dictionary<ITable<T>, List<T>> GetTableByGroupModelList(IEnumerable<T> modelList)
        {
            throw new Exception(_msg);
        }

        public override bool Insert(T model)
        {
            return Wrap(tran =>
            {
                var tb = tran.GetTranTable(_GetTableByModel(model));
                return tb.Insert(model);
            });
        }

        public override void Insert(IEnumerable<T> modelList)
        {
            Wrap(tran =>
            {
                var dict = _GetTableByGroupModelList(modelList);
                foreach (var item in dict)
                {
                    var tb = tran.GetTranTable(item.Key);
                    tb.Insert(item.Value);
                }
            });
        }

        public override void InsertIfNoExists(T model)
        {
            if (!Exists(model))
            {
                Insert(model);
            }
        }

        public override void InsertIfNoExists(IEnumerable<T> modelList)
        {
            foreach (var item in modelList)
            {
                InsertIfNoExists(item);
            }
        }

        public override bool InsertIdentity(T model)
        {
            return Wrap(tran =>
            {
                var tb = tran.GetTranTable(_GetTableByModel(model));
                return tb.InsertIdentity(model);
            });
        }

        public override void InsertIdentity(IEnumerable<T> modelList)
        {
            Wrap(tran =>
            {
                var dict = _GetTableByGroupModelList(modelList);
                foreach (var item in dict)
                {
                    var tb = tran.GetTranTable(item.Key);
                    tb.InsertIdentity(item.Value);
                }
            });
        }

        public override void InsertIdentityIfNoExists(T model)
        {
            if (!Exists(model))
            {
                InsertIdentity(model);
            }
        }

        public override void InsertIdentityIfNoExists(IEnumerable<T> modelList)
        {
            foreach (var item in modelList)
            {
                InsertIdentityIfNoExists(item);
            }
        }

        public override bool Update(T model, List<string> fields = null)
        {
            int count = 0;
            Wrap(tran =>
            {
                foreach (var item in TableList)
                {
                    var tb = tran.GetTranTable(item);
                    count += tb.Update(model, fields) ? 1 : 0;
                }
            });
            return count > 0;
        }

        public override void Update(IEnumerable<T> modelList, List<string> fields = null)
        {
            Wrap(tran =>
            {
                foreach (var item in TableList)
                {
                    var tb = tran.GetTranTable(item);
                    tb.Update(modelList, fields);
                }
            });
        }

        public override bool UpdateIgnore(T model, List<string> fields)
        {
            int count = 0;
            Wrap(tran =>
            {
                foreach (var item in TableList)
                {
                    var tb = tran.GetTranTable(item);
                    count += tb.UpdateIgnore(model, fields) ? 1 : 0;
                }
            });
            return count > 0;
        }

        public override void UpdateIgnore(IEnumerable<T> modelList, List<string> fields)
        {
            Wrap(tran =>
            {
                foreach (var item in TableList)
                {
                    var tb = tran.GetTranTable(item);
                    tb.UpdateIgnore(modelList, fields);
                }
            });
        }

        public override void Merge(T model, List<string> fields = null)
        {
            if (Exists(model))
            {
                Update(model, fields);
            }
            else
            {
                Insert(model);
            }
        }

        public override void Merge(IEnumerable<T> modelList, List<string> fields = null)
        {
            foreach (var item in modelList)
            {
                Merge(item, fields);
            }
        }

        public override void MergeIgnore(T model, List<string> fields)
        {
            if (Exists(model))
            {
                UpdateIgnore(model, fields);
            }
            else
            {
                Insert(model);
            }
        }

        public override void MergeIgnore(IEnumerable<T> modelList, List<string> fields)
        {
            foreach (var item in modelList)
            {
                MergeIgnore(item, fields);
            }
        }

        public override bool Delete(object id)
        {
            int count = 0;
            Wrap(tran =>
            {
                foreach (var item in TableList)
                {
                    var tb = tran.GetTranTable(item);
                    count += tb.Delete(id) ? 1 : 0;
                }
            });
            return count > 0;
        }

        public override int DeleteByIds(object ids)
        {
            int count = 0;
            Wrap(tran =>
            {
                foreach (var item in TableList)
                {
                    var tb = tran.GetTranTable(item);
                    count += tb.DeleteByIds(ids);
                }
            });
            return count;
        }

        public override void Delete(T model)
        {
            Wrap(tran =>
            {
                foreach (var item in TableList)
                {
                    var tb = tran.GetTranTable(item);
                    tb.Delete(model);
                }
            });
        }

        public override void Delete(IEnumerable<T> modelList)
        {
            Wrap(tran =>
            {
                foreach (var item in TableList)
                {
                    var tb = tran.GetTranTable(item);
                    tb.Delete(modelList);
                }
            });
        }

        public override  bool Exists(object id)
        {
            return Query.ExistsAsync(id).Result;
        }

        public override bool Exists(T model)
        {
            return Query.ExistsAsync(model).Result;
        }

        public override T GetById(object id, string returnFields = null)
        {
            return Query.GetByIdAsync(id, returnFields).Result;
        }

        public override async Task<IEnumerable<T>> GetByIdsAsync(object ids, string returnFields = null)
        {
            return await Query.GetByIdsAsync(ids, returnFields);
        }

        #endregion

        #region abstract

        public override ISharding<T> CreateTranSharding(DistributedTransaction tran)
        {
            return new AutoSharding<T>(TableList, tran);
        }

        public override ITable<T> GetTableById(object id)
        {
            var taskList = TableList.Select(s =>
            {
                return Task.Run(() =>
                {
                    return s.Exists(id);
                });
            });

            var result = Task.WhenAll(taskList).Result;
            for (int i = 0; i < result.Length; i++)
            {
                if (result[i])
                {
                    return TableList[i];
                }
            }
            return null;
        }

        public override ITable<T> GetTableByModel(T model)
        {
            var accessor = TypeAccessor.Create(typeof(T));
            var id = accessor[model, SqlField.PrimaryKey];
            return GetTableById(id);
        }

        #endregion


    }
}
