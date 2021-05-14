using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Dapper.Sharding
{
    public class ShardingQueryDb
    {
        public ShardingQueryDb(params IDatabase[] dbList)
        {
            DbList = dbList;
        }

        public IDatabase[] DbList { get; }

        public async Task<int[]> ExecuteAsync(string sql, object param = null)
        {
            var taskList = DbList.Select(s =>
            {
                return Task.Run(() =>
                {
                    using (var conn = s.GetConn())
                    {
                        return conn.Execute(sql, param);
                    }
                });
            });
            return await Task.WhenAll(taskList);
        }

        public async Task<object[]> ExecuteScalarAsync(string sql, object param = null)
        {
            var taskList = DbList.Select(s =>
            {
                return Task.Run(() =>
                {
                    using (var conn = s.GetConn())
                    {
                        return conn.ExecuteScalar(sql, param);
                    }  
                });
            });
            return await Task.WhenAll(taskList);
        }

        public async Task<TResult[]> ExecuteScalarAsync<TResult>(string sql, object param = null)
        {
            var taskList = DbList.Select(s =>
            {
                return Task.Run(() =>
                {
                    using (var conn = s.GetConn())
                    {
                        return conn.ExecuteScalar<TResult>(sql, param);
                    }     
                });
            });
            return await Task.WhenAll(taskList);
        }

        public async Task<dynamic[]> QueryFirstOrDefaultAsync(string sql, object param = null)
        {
            var taskList = DbList.Select(s =>
            {
                return Task.Run(() =>
                {
                    using (var conn = s.GetConn())
                    {
                        return conn.QueryFirstOrDefault(sql, param);
                    }                    
                });
            });
            return await Task.WhenAll(taskList);
        }

        public async Task<TResult[]> QueryFirstOrDefaultAsync<TResult>(string sql, object param = null)
        {
            var taskList = DbList.Select(s =>
            {
                return Task.Run(() =>
                {
                    using (var conn = s.GetConn())
                    {
                        return conn.QueryFirstOrDefault<TResult>(sql, param);
                    }                  
                });
            });
            return await Task.WhenAll(taskList);
        }

        public async Task<IEnumerable<dynamic>[]> QueryAsync(string sql, object param = null)
        {
            var taskList = DbList.Select(s =>
            {
                return Task.Run(() =>
                {
                    using (var conn = s.GetConn())
                    {
                        return conn.Query(sql, param);
                    }                   
                });
            });
            return await Task.WhenAll(taskList);
        }

        public async Task<IEnumerable<TResult>[]> QueryAsync<TResult>(string sql, object param = null)
        {
            var taskList = DbList.Select(s =>
            {
                return Task.Run(() =>
                {
                    using (var conn = s.GetConn())
                    {
                        return conn.Query<TResult>(sql, param);
                    }              
                });
            });
            return await Task.WhenAll(taskList);
        }
    }
}
