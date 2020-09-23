using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Dapper.Sharding
{
    public class ShardingQuery<T>
    {
        public ShardingQuery(params ITable<T>[] tableList)
        {
            TableList = tableList;
        }

        public ITable<T>[] TableList { get; }

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

        public long Count()
        {
            var taskList = TableList.Select(s =>
            {
                return Task.Run(() =>
                {
                    return s.Count();
                });
            });

            var result = Task.WhenAll(taskList).Result;
            return result.Sum();
        }

        public long Count(string where, object param = null)
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

        public IEnumerable<T> GetAll(string returnFields = null)
        {
            var taskList = TableList.Select(s =>
            {
                return Task.Run(() =>
                {
                    return s.GetAll(returnFields);
                });
            });
            var result = Task.WhenAll(taskList).Result;
            var list = Enumerable.Empty<T>();
            foreach (var item in result)
            {
                list = list.Concat(item);
            }
            return list;
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
            return result.FirstOrDefault();
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
            var list = Enumerable.Empty<T>();
            foreach (var item in result)
            {
                list = list.Concat(item);
            }
            return list;
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
            var list = Enumerable.Empty<T>();
            foreach (var item in result)
            {
                list = list.Concat(item);
            }
            return list;
        }

        public IEnumerable<T> GetByWhere(string where, object param = null, string returnFields = null)
        {
            var taskList = TableList.Select(s =>
            {
                return Task.Run(() =>
                {
                    return s.GetByWhere(where, param, returnFields);
                });
            });
            var result = Task.WhenAll(taskList).Result;
            var list = Enumerable.Empty<T>();
            foreach (var item in result)
            {
                list = list.Concat(item);
            }
            return list;
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
            return result.FirstOrDefault();
        }

        public IEnumerable<T> GetBySkipTake(int skip, int take, string where = null, object param = null, string returnFields = null)
        {
            var taskList = TableList.Select(s =>
            {
                return Task.Run(() =>
                {
                    return s.GetBySkipTake(skip, take, where, param, returnFields);
                });
            });
            var result = Task.WhenAll(taskList).Result;
            return null;

        }

        public IEnumerable<T> GetByPage(int page, int pageSize, string where = null, object param = null, string returnFields = null)
        {
            return default;
        }

        public IEnumerable<T> GetByPageAndCount(int page, int pageSize, out long count, string where = null, object param = null, string returnFields = null)
        {
            count = default;
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

    }
}
