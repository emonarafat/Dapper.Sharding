using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Linq.Dynamic.Core;

namespace Dapper.Sharding
{
    public class ShardingQuery<T> : ICommon<T> where T : class
    {
        public ShardingQuery(params ITable<T>[] tableList)
        {
            TableList = tableList;
            SqlField = tableList[0].SqlField;
            DataBase = tableList[0].DataBase;
        }

        public SqlFieldEntity SqlField { get; }

        public ITable<T>[] TableList { get; }

        public IDatabase DataBase { get; }

        public bool Exists(object id)
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

        public bool Exists(T model)
        {
            var taskList = TableList.Select(s =>
            {
                return Task.Run(() =>
                {
                    return s.Exists(model);
                });
            });

            var result = Task.WhenAll(taskList).Result;
            return result.Any(a => a == true);
        }


        public long Count(string where = null, object param = null)
        {
            var taskList = TableList.Select(s =>
            {
                return Task.Run(() =>
                {
                    return s.Count(where, param);
                });
            });

            var result = Task.WhenAll(taskList).Result;
            return result.Sum();
        }

        public TValue Min<TValue>(string field, string where = null, object param = null)
        {
            var taskList = TableList.Select(s =>
            {
                return Task.Run(() =>
                {
                    return s.Min<TValue>(field, where, param);
                });
            });

            var result = Task.WhenAll(taskList).Result;
            return result.Min();
        }

        public TValue Max<TValue>(string field, string where = null, object param = null)
        {
            var taskList = TableList.Select(s =>
            {
                return Task.Run(() =>
                {
                    return s.Max<TValue>(field, where, param);
                });
            });

            var result = Task.WhenAll(taskList).Result;
            return result.Max();
        }

        public TValue[] SumList<TValue>(string field, string where = null, object param = null)
        {
            var taskList = TableList.Select(s =>
            {
                return Task.Run(() =>
                {
                    return s.Sum<TValue>(field, where, param);
                });
            });

            return Task.WhenAll(taskList).Result;
        }

        public int SumInt(string field, string where = null, object param = null)
        {
            return SumList<int>(field, where, param).Sum();
        }

        public long SumLong(string field, string where = null, object param = null)
        {
            return SumList<long>(field, where, param).Sum();
        }

        public float SumFloat(string field, string where = null, object param = null)
        {
            return SumList<float>(field, where, param).Sum();
        }

        public double SumDouble(string field, string where = null, object param = null)
        {
            return SumList<double>(field, where, param).Sum();
        }

        public decimal SumDecimal(string field, string where = null, object param = null)
        {
            return SumList<decimal>(field, where, param).Sum();
        }

        public decimal Avg(string field, string where = null, object param = null)
        {
            return SumDecimal(field, where, param) / Count(where, param);
        }

        public IEnumerable<T> GetAll(string returnFields = null, string orderby = null)
        {
            var taskList = TableList.Select(s =>
            {
                return Task.Run(() =>
                {
                    return s.GetAll(returnFields, orderby);
                });
            });
            if (string.IsNullOrEmpty(orderby))
            {
                orderby = SqlField.PrimaryKey;
            }
            var result = Task.WhenAll(taskList).Result;
            return result.ConcatItem().AsQueryable().OrderBy(orderby).AsEnumerable<T>();
        }

        public T GetById(object id, string returnFields = null)
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

        public IEnumerable<T> GetByIds(object ids, string returnFields = null)
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

        public IEnumerable<T> GetByIdsWithField(object ids, string field, string returnFields = null)
        {
            var taskList = TableList.Select(s =>
            {
                return Task.Run(() =>
                {
                    return s.GetByIdsWithField(ids, field, returnFields);
                });
            });
            var result = Task.WhenAll(taskList).Result;
            return result.ConcatItem();
        }

        public IEnumerable<T> GetByWhere(string where, object param = null, string returnFields = null, string orderby = null, int limit = 0)
        {
            var taskList = TableList.Select(s =>
            {
                return Task.Run(() =>
                {
                    return s.GetByWhere(where, param, returnFields, orderby, limit);
                });
            });
            var result = Task.WhenAll(taskList).Result;
            if (string.IsNullOrEmpty(orderby))
            {
                orderby = SqlField.PrimaryKey;
            }

            if (limit != 0)
            {
                return result.ConcatItem().AsQueryable().OrderBy(orderby).Take(limit).AsEnumerable<T>();
            }
            return result.ConcatItem().AsQueryable().OrderBy(orderby).AsEnumerable<T>();
        }

        public T GetByWhereFirst(string where, object param = null, string returnFields = null)
        {
            var taskList = TableList.Select(s =>
            {
                return Task.Run(() =>
                {
                    return s.GetByWhereFirst(where, param, returnFields);
                });
            });
            var result = Task.WhenAll(taskList).Result;
            return result.FirstOrDefault(f => f != null);
        }

        public IEnumerable<T> GetBySkipTake(int skip, int take, string where = null, object param = null, string returnFields = null, string orderby = null)
        {
            var taskList = TableList.Select(s =>
            {
                return Task.Run(() =>
                {
                    return s.GetByWhere(where, param, returnFields, orderby, skip + take);
                });
            });
            var result = Task.WhenAll(taskList).Result;
            if (string.IsNullOrEmpty(orderby))
            {
                orderby = SqlField.PrimaryKey;
            }
            return result.ConcatItem().AsQueryable().OrderBy(orderby).Skip(skip).Take(take).AsEnumerable<T>();
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

        public IEnumerable<T> GetByPageAndCount(int page, int pageSize, out long count, string where = null, object param = null, string returnFields = null, string orderby = null)
        {
            var data = GetByPage(page, pageSize, where, param, returnFields, orderby);
            count = Count(where, param);
            return data;
        }

        public IEnumerable<T> GetByAscFirstPage(int pageSize, object param = null, string and = null, string returnFields = null)
        {
            var taskList = TableList.Select(s =>
            {
                return Task.Run(() =>
                {
                    return s.GetByAscFirstPage(pageSize, param, and, returnFields);
                });
            });
            var result = Task.WhenAll(taskList).Result;
            return result.ConcatItem().AsQueryable().OrderBy(SqlField.PrimaryKey).Take(pageSize).AsEnumerable<T>();
        }

        public IEnumerable<T> GetByAscPrevPage(int pageSize, T param, string and = null, string returnFields = null)
        {
            var taskList = TableList.Select(s =>
            {
                return Task.Run(() =>
                {
                    return s.GetByAscPrevPage(pageSize, param, and, returnFields);
                });
            });
            var result = Task.WhenAll(taskList).Result;
            return result.ConcatItem().AsQueryable().OrderBy(SqlField.PrimaryKey + " DESC").Take(pageSize).OrderBy(SqlField.PrimaryKey).AsEnumerable<T>();
        }

        public IEnumerable<T> GetByAscCurrentPage(int pageSize, T param, string and = null, string returnFields = null)
        {
            var taskList = TableList.Select(s =>
            {
                return Task.Run(() =>
                {
                    return s.GetByAscCurrentPage(pageSize, param, and, returnFields);
                });
            });
            var result = Task.WhenAll(taskList).Result;
            return result.ConcatItem().AsQueryable().OrderBy(SqlField.PrimaryKey).Take(pageSize).AsEnumerable<T>();
        }

        public IEnumerable<T> GetByAscNextPage(int pageSize, T param, string and = null, string returnFields = null)
        {
            var taskList = TableList.Select(s =>
            {
                return Task.Run(() =>
                {
                    return s.GetByAscNextPage(pageSize, param, and, returnFields);
                });
            });
            var result = Task.WhenAll(taskList).Result;
            return result.ConcatItem().AsQueryable().OrderBy(SqlField.PrimaryKey).Take(pageSize).AsEnumerable<T>();
        }

        public IEnumerable<T> GetByAscLastPage(int pageSize, object param = null, string and = null, string returnFields = null)
        {
            var taskList = TableList.Select(s =>
            {
                return Task.Run(() =>
                {
                    return s.GetByAscLastPage(pageSize, param, and, returnFields);
                });
            });
            var result = Task.WhenAll(taskList).Result;
            return result.ConcatItem().AsQueryable().OrderBy(SqlField.PrimaryKey + " DESC").Take(pageSize).OrderBy(SqlField.PrimaryKey).AsEnumerable<T>();
        }

        public IEnumerable<T> GetByDescFirstPage(int pageSize, object param = null, string and = null, string returnFields = null)
        {
            var taskList = TableList.Select(s =>
            {
                return Task.Run(() =>
                {
                    return s.GetByDescFirstPage(pageSize, param, and, returnFields);
                });
            });
            var result = Task.WhenAll(taskList).Result;
            return result.ConcatItem().AsQueryable().OrderBy(SqlField.PrimaryKey + " DESC").Take(pageSize).AsEnumerable<T>();
        }

        public IEnumerable<T> GetByDescPrevPage(int pageSize, T param, string and = null, string returnFields = null)
        {
            var taskList = TableList.Select(s =>
            {
                return Task.Run(() =>
                {
                    return s.GetByDescPrevPage(pageSize, param, and, returnFields);
                });
            });
            var result = Task.WhenAll(taskList).Result;
            return result.ConcatItem().AsQueryable().OrderBy(SqlField.PrimaryKey).Take(pageSize).OrderBy(SqlField.PrimaryKey + " DESC").AsEnumerable<T>();
        }

        public IEnumerable<T> GetByDescCurrentPage(int pageSize, T param, string and = null, string returnFields = null)
        {
            var taskList = TableList.Select(s =>
            {
                return Task.Run(() =>
                {
                    return s.GetByDescCurrentPage(pageSize, param, and, returnFields);
                });
            });
            var result = Task.WhenAll(taskList).Result;
            return result.ConcatItem().AsQueryable().OrderBy(SqlField.PrimaryKey + " DESC").Take(pageSize).AsEnumerable<T>();
        }

        public IEnumerable<T> GetByDescNextPage(int pageSize, T param, string and = null, string returnFields = null)
        {
            var taskList = TableList.Select(s =>
            {
                return Task.Run(() =>
                {
                    return s.GetByDescNextPage(pageSize, param, and, returnFields);
                });
            });
            var result = Task.WhenAll(taskList).Result;
            return result.ConcatItem().AsQueryable().OrderBy(SqlField.PrimaryKey + " DESC").Take(pageSize).AsEnumerable<T>();
        }

        public IEnumerable<T> GetByDescLastPage(int pageSize, object param = null, string and = null, string returnFields = null)
        {
            var taskList = TableList.Select(s =>
            {
                return Task.Run(() =>
                {
                    return s.GetByDescLastPage(pageSize, param, and, returnFields);
                });
            });
            var result = Task.WhenAll(taskList).Result;
            return result.ConcatItem().AsQueryable().OrderBy(SqlField.PrimaryKey).Take(pageSize).OrderBy(SqlField.PrimaryKey + " DESC").AsEnumerable<T>();

        }

    }
}
