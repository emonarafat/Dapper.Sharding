using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Linq.Dynamic.Core;
using System.Xml.Serialization;

namespace Dapper.Sharding
{
    public class ShardingQuery<T> where T : class
    {
        public ShardingQuery(params ITable<T>[] tableList)
        {
            TableList = tableList;
            SqlField = tableList[0].SqlField;
        }

        public SqlFieldEntity SqlField { get; }

        public ITable<T>[] TableList { get; }

        public async Task<bool> ExistsAsync(object id)
        {
            var taskList = TableList.Select(s =>
            {
                return Task.Run(() =>
                {
                    return s.Exists(id);
                });
            });

            var result = await Task.WhenAll(taskList);
            return result.Any(a => a == true);
        }

        public async Task<bool> ExistsAsync(T model)
        {
            var taskList = TableList.Select(s =>
            {
                return Task.Run(() =>
                {
                    return s.Exists(model);
                });
            });

            var result = await Task.WhenAll(taskList);
            return result.Any(a => a == true);
        }

        public async Task<long> CountAsync(string where = null, object param = null)
        {
            var taskList = TableList.Select(s =>
            {
                return Task.Run(() =>
                {
                    return s.Count(where, param);
                });
            });

            var result = await Task.WhenAll(taskList);
            return result.Sum();
        }

        public async Task<TResult> MinAsync<TResult>(string field, string where = null, object param = null)
        {
            var taskList = TableList.Select(s =>
            {
                return Task.Run(() =>
                {
                    return s.Min<TResult>(field, where, param);
                });
            });

            var result = await Task.WhenAll(taskList);
            return result.Min();
        }

        public async Task<TResult> MaxAsync<TResult>(string field, string where = null, object param = null)
        {
            var taskList = TableList.Select(s =>
            {
                return Task.Run(() =>
                {
                    return s.Max<TResult>(field, where, param);
                });
            });

            var result = await Task.WhenAll(taskList);
            return result.Max();
        }

        public async Task<TResult[]> SumListAsync<TResult>(string field, string where = null, object param = null)
        {
            var taskList = TableList.Select(s =>
            {
                return Task.Run(() =>
                {
                    return s.Sum<TResult>(field, where, param);
                });
            });

            return await Task.WhenAll(taskList);
        }

        public async Task<int> SumIntAsync(string field, string where = null, object param = null)
        {
            var data = await SumListAsync<int>(field, where, param);
            return data.Sum();
        }

        public async Task<long> SumLongAsync(string field, string where = null, object param = null)
        {
            var data = await SumListAsync<long>(field, where, param);
            return data.Sum();
        }

        public async Task<float> SumFloatAsync(string field, string where = null, object param = null)
        {
            var data = await SumListAsync<float>(field, where, param);
            return data.Sum();
        }

        public async Task<double> SumDoubleAsync(string field, string where = null, object param = null)
        {
            var data = await SumListAsync<double>(field, where, param);
            return data.Sum();
        }

        public async Task<decimal> SumDecimalAsync(string field, string where = null, object param = null)
        {
            var data = await SumListAsync<decimal>(field, where, param);
            return data.Sum();
        }

        public async Task<decimal> AvgAsync(string field, string where = null, object param = null)
        {
            var count = await CountAsync(where, param);
            if (count == 0)
            {
                return 0;
            }
            var sum = await SumDecimalAsync(field, where, param);     
            return sum / count;
        }

        public async Task<IEnumerable<T>> GetAllAsync(string returnFields = null, string orderby = null)
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
            var result = await Task.WhenAll(taskList);
            return result.ConcatItem().AsQueryable().OrderBy(orderby).AsEnumerable<T>();
        }

        public async Task<T> GetByIdAsync(object id, string returnFields = null)
        {
            var taskList = TableList.Select(s =>
            {
                return Task.Run(() =>
                {
                    return s.GetById(id, returnFields);
                });
            });
            var result = await Task.WhenAll(taskList);
            return result.FirstOrDefault(f => f != null);
        }

        public async Task<IEnumerable<T>> GetByIdsAsync(object ids, string returnFields = null)
        {
            var taskList = TableList.Select(s =>
            {
                return Task.Run(() =>
                {
                    return s.GetByIds(ids, returnFields);
                });
            });
            var result = await Task.WhenAll(taskList);
            return result.ConcatItem();
        }

        public async Task<IEnumerable<T>> GetByIdsWithFieldAsync(object ids, string field, string returnFields = null)
        {
            var taskList = TableList.Select(s =>
            {
                return Task.Run(() =>
                {
                    return s.GetByIdsWithField(ids, field, returnFields);
                });
            });
            var result = await Task.WhenAll(taskList);
            return result.ConcatItem();
        }

        public async Task<IEnumerable<T>> GetByWhereAsync(string where, object param = null, string returnFields = null, string orderby = null, int limit = 0)
        {
            var taskList = TableList.Select(s =>
            {
                return Task.Run(() =>
                {
                    return s.GetByWhere(where, param, returnFields, orderby, limit);
                });
            });
            var result = await Task.WhenAll(taskList);
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

        public async Task<T> GetByWhereFirstAsync(string where, object param = null, string returnFields = null)
        {
            var taskList = TableList.Select(s =>
            {
                return Task.Run(() =>
                {
                    return s.GetByWhereFirst(where, param, returnFields);
                });
            });
            var result = await Task.WhenAll(taskList);
            return result.FirstOrDefault(f => f != null);
        }

        public async Task<IEnumerable<T>> GetBySkipTakeAsync(int skip, int take, string where = null, object param = null, string returnFields = null, string orderby = null)
        {
            var taskList = TableList.Select(s =>
            {
                return Task.Run(() =>
                {
                    return s.GetByWhere(where, param, returnFields, orderby, skip + take);
                });
            });
            var result = await Task.WhenAll(taskList);
            if (string.IsNullOrEmpty(orderby))
            {
                orderby = SqlField.PrimaryKey;
            }
            return result.ConcatItem().AsQueryable().OrderBy(orderby).Skip(skip).Take(take).AsEnumerable<T>();
        }

        public async Task<IEnumerable<T>> GetByPageAsync(int page, int pageSize, string where = null, object param = null, string returnFields = null, string orderby = null)
        {
            int skip = 0;
            if (page > 0)
            {
                skip = (page - 1) * pageSize;
            }
            return await GetBySkipTakeAsync(skip, pageSize, where, param, returnFields, orderby);
        }

        public async Task<PageEntity<T>> GetByPageAndCountAsync(int page, int pageSize, string where = null, object param = null, string returnFields = null, string orderby = null)
        {
            var task1 = GetByPageAsync(page, pageSize, where, param, returnFields, orderby);
            var task2 = CountAsync(where, param);
            await Task.WhenAll(task1, task2);
            var pageEntity = new PageEntity<T>();
            pageEntity.Data = task1.Result;
            pageEntity.Count = task2.Result;
            return pageEntity;
        }

        public async Task<IEnumerable<T>> GetByAscFirstPageAsync(int pageSize, object param = null, string and = null, string returnFields = null)
        {
            var taskList = TableList.Select(s =>
            {
                return Task.Run(() =>
                {
                    return s.GetByAscFirstPage(pageSize, param, and, returnFields);
                });
            });
            var result = await Task.WhenAll(taskList);
            return result.ConcatItem().AsQueryable().OrderBy(SqlField.PrimaryKey).Take(pageSize).AsEnumerable<T>();
        }

        public async Task<IEnumerable<T>> GetByAscPrevPageAsync(int pageSize, T param, string and = null, string returnFields = null)
        {
            var taskList = TableList.Select(s =>
            {
                return Task.Run(() =>
                {
                    return s.GetByAscPrevPage(pageSize, param, and, returnFields);
                });
            });
            var result = await Task.WhenAll(taskList);
            return result.ConcatItem().AsQueryable().OrderBy(SqlField.PrimaryKey + " DESC").Take(pageSize).OrderBy(SqlField.PrimaryKey).AsEnumerable<T>();
        }

        public async Task<IEnumerable<T>> GetByAscCurrentPageAsync(int pageSize, T param, string and = null, string returnFields = null)
        {
            var taskList = TableList.Select(s =>
            {
                return Task.Run(() =>
                {
                    return s.GetByAscCurrentPage(pageSize, param, and, returnFields);
                });
            });
            var result = await Task.WhenAll(taskList);
            return result.ConcatItem().AsQueryable().OrderBy(SqlField.PrimaryKey).Take(pageSize).AsEnumerable<T>();
        }

        public async Task<IEnumerable<T>> GetByAscNextPageAsync(int pageSize, T param, string and = null, string returnFields = null)
        {
            var taskList = TableList.Select(s =>
            {
                return Task.Run(() =>
                {
                    return s.GetByAscNextPage(pageSize, param, and, returnFields);
                });
            });
            var result = await Task.WhenAll(taskList);
            return result.ConcatItem().AsQueryable().OrderBy(SqlField.PrimaryKey).Take(pageSize).AsEnumerable<T>();
        }

        public async Task<IEnumerable<T>> GetByAscLastPageAsync(int pageSize, object param = null, string and = null, string returnFields = null)
        {
            var taskList = TableList.Select(s =>
            {
                return Task.Run(() =>
                {
                    return s.GetByAscLastPage(pageSize, param, and, returnFields);
                });
            });
            var result = await Task.WhenAll(taskList);
            return result.ConcatItem().AsQueryable().OrderBy(SqlField.PrimaryKey + " DESC").Take(pageSize).OrderBy(SqlField.PrimaryKey).AsEnumerable<T>();
        }

        public async Task<IEnumerable<T>> GetByDescFirstPageAsync(int pageSize, object param = null, string and = null, string returnFields = null)
        {
            var taskList = TableList.Select(s =>
            {
                return Task.Run(() =>
                {
                    return s.GetByDescFirstPage(pageSize, param, and, returnFields);
                });
            });
            var result = await Task.WhenAll(taskList);
            return result.ConcatItem().AsQueryable().OrderBy(SqlField.PrimaryKey + " DESC").Take(pageSize).AsEnumerable<T>();
        }

        public async Task<IEnumerable<T>> GetByDescPrevPageAsync(int pageSize, T param, string and = null, string returnFields = null)
        {
            var taskList = TableList.Select(s =>
            {
                return Task.Run(() =>
                {
                    return s.GetByDescPrevPage(pageSize, param, and, returnFields);
                });
            });
            var result = await Task.WhenAll(taskList);
            return result.ConcatItem().AsQueryable().OrderBy(SqlField.PrimaryKey).Take(pageSize).OrderBy(SqlField.PrimaryKey + " DESC").AsEnumerable<T>();
        }

        public async Task<IEnumerable<T>> GetByDescCurrentPageAsync(int pageSize, T param, string and = null, string returnFields = null)
        {
            var taskList = TableList.Select(s =>
            {
                return Task.Run(() =>
                {
                    return s.GetByDescCurrentPage(pageSize, param, and, returnFields);
                });
            });
            var result = await Task.WhenAll(taskList);
            return result.ConcatItem().AsQueryable().OrderBy(SqlField.PrimaryKey + " DESC").Take(pageSize).AsEnumerable<T>();
        }

        public async Task<IEnumerable<T>> GetByDescNextPageAsync(int pageSize, T param, string and = null, string returnFields = null)
        {
            var taskList = TableList.Select(s =>
            {
                return Task.Run(() =>
                {
                    return s.GetByDescNextPage(pageSize, param, and, returnFields);
                });
            });
            var result = await Task.WhenAll(taskList);
            return result.ConcatItem().AsQueryable().OrderBy(SqlField.PrimaryKey + " DESC").Take(pageSize).AsEnumerable<T>();
        }

        public async Task<IEnumerable<T>> GetByDescLastPageAsync(int pageSize, object param = null, string and = null, string returnFields = null)
        {
            var taskList = TableList.Select(s =>
            {
                return Task.Run(() =>
                {
                    return s.GetByDescLastPage(pageSize, param, and, returnFields);
                });
            });
            var result = await Task.WhenAll(taskList);
            return result.ConcatItem().AsQueryable().OrderBy(SqlField.PrimaryKey).Take(pageSize).OrderBy(SqlField.PrimaryKey + " DESC").AsEnumerable<T>();

        }

        #region dapper method

        private static string InitTable(string sql, string name)
        {
            return sql.Replace("$table", name);
        }

        public async Task<int[]> ExecuteAsync(string sql, object param = null, int? commandTimeout = null)
        {
            var taskList = TableList.Select(s =>
            {
                return s.DpEntity.ExecuteAsync(InitTable(sql, s.Name), param, commandTimeout);
            });
            return await Task.WhenAll(taskList);
        }

        public async Task<object[]> ExecuteScalarAsync(string sql, object param = null, int? commandTimeout = null)
        {
            var taskList = TableList.Select(s =>
            {
                return s.DpEntity.ExecuteScalarAsync(InitTable(sql, s.Name), param, commandTimeout);
            });
            return await Task.WhenAll(taskList);
        }

        public async Task<TResult[]> ExecuteScalarAsync<TResult>(string sql, object param = null, int? commandTimeout = null)
        {
            var taskList = TableList.Select(s =>
            {
                return s.DpEntity.ExecuteScalarAsync<TResult>(InitTable(sql, s.Name), param, commandTimeout);
            });
            return await Task.WhenAll(taskList);
        }

        public async Task<dynamic[]> QueryFirstOrDefaultAsync(string sql, object param = null, int? commandTimeout = null)
        {
            var taskList = TableList.Select(s =>
            {
                return s.DpEntity.QueryFirstOrDefaultAsync(InitTable(sql, s.Name), param, commandTimeout);
            });
            return await Task.WhenAll(taskList);
        }

        public async Task<TResult[]> QueryFirstOrDefaultAsync<TResult>(string sql, object param = null, int? commandTimeout = null)
        {
            var taskList = TableList.Select(s =>
            {
                return s.DpEntity.QueryFirstOrDefaultAsync<TResult>(InitTable(sql, s.Name), param, commandTimeout);
            });
            return await Task.WhenAll(taskList);
        }

        public async Task<IEnumerable<dynamic>[]> QueryAsync(string sql, object param = null, int? commandTimeout = null)
        {
            var taskList = TableList.Select(s =>
            {
                return s.DpEntity.QueryAsync(InitTable(sql, s.Name), param, commandTimeout);
            });
            return await Task.WhenAll(taskList);
        }

        public async Task<IEnumerable<TResult>[]> QueryAsync<TResult>(string sql, object param = null, int? commandTimeout = null)
        {
            var taskList = TableList.Select(s =>
            {
                return s.DpEntity.QueryAsync<TResult>(InitTable(sql, s.Name), param, commandTimeout);
            });
            return await Task.WhenAll(taskList);
        }

        #endregion


    }
}
