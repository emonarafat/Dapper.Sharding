using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Dapper.Sharding
{
    internal class HashSharding<T> : ISharding<T>
    {
        public HashSharding(ITable<T>[] tableList) : base(tableList)
        {

        }

        public override ITable<T> GetTableById(string id)
        {
            return TableList[ShardingUtils.Mod(id, TableList.Length)];
        }

        public override ITable<T> GetTableById(object id)
        {
            return TableList[ShardingUtils.Mod(id, TableList.Length)];
        }

        public override ITable<T> GetTableByModel(T model)
        {
            return TableList[ShardingUtils.Mod(model, KeyName, KeyType, TableList.Length)];
        }

        public override ITable<T> GetTableByModelAndInitId(T model)
        {
            return TableList[ShardingUtils.ModAndInitId(model, KeyName, KeyType, TableList.Length)];
        }

        public override bool Delete(object id)
        {
            return GetTableById(id).Delete(id);
        }

        public override int DeleteByIds(object ids)
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

        public override bool Exists(object id)
        {
            return GetTableById(id).Exists(id);
        }

        public override T GetById(object id, string returnFields = null)
        {
            return GetTableById(id).GetById(id, returnFields);
        }

        public override IEnumerable<T> GetByIds(object ids, string returnFields = null)
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

        public override bool Insert(T model)
        {
            return GetTableByModelAndInitId(model).Insert(model);
        }

        public override bool InsertIfExistsUpdate(T model, string fields = null)
        {
            return GetTableByModel(model).InsertIfExistsUpdate(model, fields);
        }

        public override bool InsertIfNoExists(T model)
        {
            return GetTableByModel(model).InsertIdentityIfNoExists(model);
        }

        public override int InsertMany(IEnumerable<T> modelList)
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

        public override bool Update(T model)
        {
            return GetTableByModel(model).Update(model);
        }

        public override bool UpdateExclude(T model, string fields)
        {
            return GetTableByModel(model).UpdateExclude(model, fields);
        }

        public override int UpdateExcludeMany(IEnumerable<T> modelList, string fields)
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

        public override bool UpdateInclude(T model, string fields)
        {
            return GetTableByModel(model).UpdateInclude(model, fields);
        }

        public override int UpdateIncludeMany(IEnumerable<T> modelList, string fields)
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

        public override int UpdateMany(IEnumerable<T> modelList)
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
    }
}
