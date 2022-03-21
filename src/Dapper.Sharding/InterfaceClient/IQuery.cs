using System.Collections.Generic;
using System.Data;
using System.Text;
using System.Threading.Tasks;

namespace Dapper.Sharding
{
    public abstract class IQuery
    {
        protected IDatabase db;
        protected string primaryKey;
        protected string returnFields;
        protected string _sql;
        protected string _sqlCount;
        protected int skip;
        protected int take;
        protected bool EnableCount = false;
        object par;
        DistributedTransaction tran;
        int? timeout;
        public IQuery(IDatabase db)
        {
            this.db = db;
        }

        #region Abstract

        protected string sqlTable;
        internal abstract IQuery Add<T>(ITable<T> table, string asName = null) where T : class;

        public abstract IQuery LeftJoin<T>(ITable<T> table, string asName, string on) where T : class;

        public abstract IQuery InnerJoin<T>(ITable<T> table, string asName, string on) where T : class;

        public abstract IQuery Union<T>(ITable<T> table) where T : class;

        public abstract IQuery UnionAll<T>(ITable<T> table) where T : class;

        internal abstract void Build();

        #endregion

        #region GetSQL

        public string GetSql()
        {
            Build();
            return _sql;
        }

        public string GetSqlCount()
        {
            EnableCount = true;
            Build();
            return _sqlCount;
        }

        #endregion

        #region Query Condition

        protected string sqlWhere;
        public IQuery Where(string where)
        {
            sqlWhere = where;
            return this;
        }

        protected string sqlOrderBy;
        public IQuery OrderBy(string orderBy)
        {
            sqlOrderBy = orderBy;
            return this;
        }

        public IQuery ReturnFields(string fields)
        {
            returnFields = fields;
            return this;
        }

        public IQuery Param(object par, DistributedTransaction tran = null, int? timeout = null)
        {
            this.par = par;
            this.tran = tran;
            this.timeout = timeout;
            return this;
        }

        #endregion

        #region Query

        public long Count()
        {
            return db.ExecuteScalar<long>(_sqlCount, par, tran, timeout);
        }

        public Task<long> CountAsync()
        {
            return db.ExecuteScalarAsync<long>(_sqlCount, par, tran, timeout);
        }

        public T QueryFirstOrDefault<T>()
        {
            Build();
            return db.QueryFirstOrDefault<T>(_sql, par, tran, timeout);
        }

        public Task<T> QueryFirstOrDefaultAsync<T>()
        {
            Build();
            return db.QueryFirstOrDefaultAsync<T>(_sql, par, tran, timeout);
        }

        public dynamic QueryFirstOrDefault()
        {
            Build();
            return db.QueryFirstOrDefault(_sql, par, tran, timeout);
        }

        public Task<dynamic> QueryFirstOrDefaultAsync()
        {
            Build();
            return db.QueryFirstOrDefaultAsync(_sql, par, tran, timeout);
        }

        public IEnumerable<T> Query<T>()
        {
            Build();
            return db.Query<T>(_sql, par, tran, timeout);
        }

        public Task<IEnumerable<T>> QueryAsync<T>()
        {
            Build();
            return db.QueryAsync<T>(_sql, par, tran, timeout);
        }

        public IEnumerable<dynamic> Query()
        {
            Build();
            return db.Query(_sql, par, tran, timeout);
        }

        public Task<IEnumerable<dynamic>> QueryAsync()
        {
            Build();
            return db.QueryAsync(_sql, par, tran, timeout);
        }

        public DataTable QueryDataTable()
        {
            Build();
            return db.QueryDataTable(_sql, par, tran, timeout);
        }

        public Task<DataTable> QueryDataTableAsync()
        {
            Build();
            return db.QueryDataTableAsync(_sql, par, tran, timeout);
        }

        public T ExecuteScalar<T>()
        {
            Build();
            return db.ExecuteScalar<T>(_sql, par, tran, timeout);
        }

        public Task<T> ExecuteScalarAsync<T>()
        {
            Build();
            return db.ExecuteScalarAsync<T>(_sql, par, tran, timeout);
        }

        public object ExecuteScalar()
        {
            Build();
            return db.ExecuteScalar(_sql, par, tran, timeout);
        }

        public Task<object> ExecuteScalarAsync()
        {
            Build();
            return db.ExecuteScalarAsync(_sql, par, tran, timeout);
        }

        /**********QuerySkipTake************/
        public IEnumerable<T> QuerySkipTake<T>(int skip, int take)
        {
            this.skip = skip;
            this.take = take;
            Build();
            return db.Query<T>(_sql, par, tran, timeout);
        }

        public Task<IEnumerable<T>> QuerySkipTakeAsync<T>(int skip, int take)
        {
            this.skip = skip;
            this.take = take;
            Build();
            return db.QueryAsync<T>(_sql, par, tran, timeout);
        }

        public IEnumerable<dynamic> QuerySkipTake(int skip, int take)
        {
            this.skip = skip;
            this.take = take;
            Build();
            return db.Query(_sql, par, tran, timeout);
        }

        public Task<IEnumerable<dynamic>> QuerySkipTakeAsync(int skip, int take)
        {
            this.skip = skip;
            this.take = take;
            Build();
            return db.QueryAsync(_sql, par, tran, timeout);
        }

        /**********QueryPage************/
        public IEnumerable<T> QueryPage<T>(int page, int pageSize)
        {
            int skip = 0;
            if (page > 0)
            {
                skip = (page - 1) * pageSize;
            }
            return QuerySkipTake<T>(skip, pageSize);
        }

        public Task<IEnumerable<T>> QueryPageAsync<T>(int page, int pageSize)
        {
            int skip = 0;
            if (page > 0)
            {
                skip = (page - 1) * pageSize;
            }
            return QuerySkipTakeAsync<T>(skip, pageSize);
        }

        public IEnumerable<dynamic> QueryPage(int page, int pageSize)
        {
            int skip = 0;
            if (page > 0)
            {
                skip = (page - 1) * pageSize;
            }
            return QuerySkipTake(skip, pageSize);
        }

        public Task<IEnumerable<dynamic>> QueryPageAsync(int page, int pageSize)
        {
            int skip = 0;
            if (page > 0)
            {
                skip = (page - 1) * pageSize;
            }
            return QuerySkipTakeAsync(skip, pageSize);
        }

        /**********QueryPageAndCount************/

        public PageEntity<T> QueryPageAndCount<T>(int page, int pageSize)
        {
            EnableCount = true;
            return new PageEntity<T>
            {
                Data = QueryPage<T>(page, pageSize),
                Count = Count()
            };
        }

        public async Task<PageEntity<T>> QueryPageAndCountAsync<T>(int page, int pageSize)
        {
            EnableCount = true;
            var task1 = QueryPageAsync<T>(page, pageSize);
            var task2 = CountAsync();
            await Task.WhenAll(task1, task2);
            return new PageEntity<T>
            {
                Data = task1.Result,
                Count = task2.Result
            };
        }

        public PageEntity<dynamic> QueryPageAndCount(int page, int pageSize)
        {
            EnableCount = true;
            return new PageEntity<dynamic>
            {
                Data = QueryPage(page, pageSize),
                Count = Count()
            };
        }

        public async Task<PageEntity<dynamic>> QueryPageAndCountAsync(int page, int pageSize)
        {
            EnableCount = true;
            var task1 = QueryPageAsync(page, pageSize);
            var task2 = CountAsync();
            await Task.WhenAll(task1, task2);
            return new PageEntity<dynamic>
            {
                Data = task1.Result,
                Count = task2.Result
            };
        }

        #endregion

    }
}
