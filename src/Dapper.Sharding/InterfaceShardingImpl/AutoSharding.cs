using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dapper.Sharding
{
    internal class AutoSharding<T> : ISharding<T>
    {
        public AutoSharding(ITable<T>[] tableList) : base(tableList)
        {

        }

        public override ITable<T> GetTableByIdForAutoSharding(object id)
        {
            var taskList = TableList.Select(s =>
            {
                return Task.Run(() =>
                {
                    return s.Exists(id);
                });
            });
            var result = Task.WhenAll(taskList).Result;
            for (int i = 0; i < TableList.Length; i++)
            {
                if (result[i])
                {
                    return TableList[i];
                }
            }
            return null;
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
            var taskList = TableList.Select(s =>
            {
                return Task.Run(() =>
                {
                    return s.Delete(id);
                });
            });
            var result = Task.WhenAll(taskList).Result;
            return result.Any(a => a == true);
        }

        public override int DeleteByIds(object ids)
        {
            var tran = BeginTran();
            try
            {
                int count = 0;
                var tables = tran.GetTableList();
                foreach (var tb in tables)
                {
                    count += tb.DeleteByIds(ids);
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
            var taskList = TableList.Select(s =>
            {
                return Task.Run(() =>
                {
                    return s.Exists(id);
                });
            });
            var result = Task.WhenAll(taskList).Result;
            return result.Any(a => a == true);
        }

        public override T GetById(object id, string returnFields = null)
        {
            var taskList = TableList.Select(s =>
            {
                return Task.Run(() =>
                {
                    return s.GetById(id, returnFields);
                });
            });
            var result = Task.WhenAll(taskList).Result;
            return result.FirstOrDefault(f => f != null);
        }

        public override IEnumerable<T> GetByIds(object ids, string returnFields = null)
        {
            var taskList = TableList.Select(s =>
            {
                return Task.Run(() =>
                {
                    return s.GetByIds(ids, returnFields);
                });
            });
            var result = Task.WhenAll(taskList).Result;
            return result.ConcatItem();
        }

        public override bool Insert(T model)
        {
            return GetTableByModelAndInitId(model).Insert(model);
        }

        public override int InsertMany(IEnumerable<T> modelList)
        {
            var tran = BeginTran();
            try
            {
                int count = 0;
                foreach (var item in modelList)
                {
                    var tb = tran.GetTableAndInitId(item);
                    count += tb.Insert(item) ? 1 : 0;
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

        public override bool InsertIfExistsUpdate(T model, string fields = null)
        {
            var accessor = TypeAccessor.Create(typeof(T));
            var id = accessor[model, KeyName];
            if (Exists(id))
            {
                if (fields == null)
                    return Update(model);
                else
                    return UpdateInclude(model, fields);
            }
            return Insert(model);
        }

        public override bool InsertIfNoExists(T model)
        {
            var accessor = TypeAccessor.Create(typeof(T));
            var id = accessor[model, KeyName];
            if (!Exists(id))
            {
                return Insert(model);
            }
            return false;

        }

        public override bool Update(T model)
        {
            var tran = BeginTran();
            try
            {
                int count = 0;
                var tables = tran.GetTableList();
                foreach (var tb in tables)
                {
                    count += tb.Update(model) ? 1 : 0;
                }
                tran.Commit();
                return count > 0;
            }
            catch (Exception ex)
            {
                tran.Rollback();
                throw ex;
            }
        }

        public override bool UpdateExclude(T model, string fields)
        {
            var tran = BeginTran();
            try
            {
                int count = 0;
                var tables = tran.GetTableList();
                foreach (var tb in tables)
                {
                    count += tb.UpdateExclude(model, fields) ? 1 : 0;
                }
                tran.Commit();
                return count > 0;
            }
            catch (Exception ex)
            {
                tran.Rollback();
                throw ex;
            }
        }

        public override int UpdateExcludeMany(IEnumerable<T> modelList, string fields)
        {
            var tran = BeginTran();
            try
            {
                int count = 0;
                var tables = tran.GetTableList();
                foreach (var tb in tables)
                {
                    count += tb.UpdateExcludeMany(modelList, fields);
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
            var tran = BeginTran();
            try
            {
                int count = 0;
                var tables = tran.GetTableList();
                foreach (var tb in tables)
                {
                    count += tb.UpdateInclude(model, fields) ? 1 : 0;
                }
                tran.Commit();
                return count > 0;
            }
            catch (Exception ex)
            {
                tran.Rollback();
                throw ex;
            }
        }

        public override int UpdateIncludeMany(IEnumerable<T> modelList, string fields)
        {
            var tran = BeginTran();
            try
            {
                int count = 0;
                var tables = tran.GetTableList();
                foreach (var tb in tables)
                {
                    count += tb.UpdateIncludeMany(modelList, fields);
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
                var tables = tran.GetTableList();
                foreach (var tb in tables)
                {
                    count += tb.UpdateMany(modelList);
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
