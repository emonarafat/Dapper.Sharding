using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;

namespace Dapper.Sharding
{
    public abstract class IQuery
    {
        protected IDatabase db;
        protected string primaryKey;
        protected string _sql;
        protected string _sqlCount;
        protected int skip;
        protected int take;
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

        internal abstract void Build();

        internal abstract void BuildCount();
        #endregion

        #region GetSQL

        public string GetSql()
        {
            Build();
            return _sql;
        }

        public string GetSqlCount()
        {
            BuildCount();
            return _sqlCount;
        }

        #endregion

        #region Query Condition

        protected string sqlWhere;
        public IQuery Where(string where)
        {
            sqlWhere = $"WHERE {where}";
            return this;
        }

        protected string sqlGroupBy;
        public IQuery GroupBy(string groupBy)
        {
            sqlGroupBy = $"GROUP BY {groupBy}";
            return this;
        }

        protected string sqlHaving;
        public IQuery Having(string having)
        {
            sqlHaving = $"HAVING {having}";
            return this;
        }

        protected string sqlOrderBy;
        public IQuery OrderBy(string orderBy)
        {
            sqlOrderBy = orderBy;
            return this;
        }

        protected string returnFields;
        public IQuery ReturnFields(string fields)
        {
            returnFields = fields;
            return this;
        }

        public IQuery Limit(int count)
        {
            skip = 0;
            take = count;
            return this;
        }

        public IQuery Limit(int skip, int take)
        {
            this.skip = skip;
            this.take = take;
            return this;
        }

        public IQuery Page(int page, int pageSize)
        {
            int skip = 0;
            if (page > 0)
            {
                skip = (page - 1) * pageSize;
            }
            this.skip = skip;
            take = pageSize;
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
            BuildCount();
            return db.ExecuteScalar<long>(_sqlCount, par, tran, timeout);
        }

        public Task<long> CountAsync()
        {
            BuildCount();
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

        /**********QueryPageAndCount************/

        public PageEntity<T> QueryPageAndCount<T>()
        {
            return new PageEntity<T>
            {
                Data = Query<T>(),
                Count = Count()
            };
        }

        public async Task<PageEntity<T>> QueryPageAndCountAsync<T>()
        {
            var task1 = QueryAsync<T>();
            var task2 = CountAsync();
            await Task.WhenAll(task1, task2);
            return new PageEntity<T>
            {
                Data = task1.Result,
                Count = task2.Result
            };
        }

        public PageEntity<dynamic> QueryPageAndCount()
        {
            return new PageEntity<dynamic>
            {
                Data = Query(),
                Count = Count()
            };
        }

        public async Task<PageEntity<dynamic>> QueryPageAndCountAsync()
        {
            var task1 = QueryAsync();
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
