using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;

namespace Dapper.Sharding
{
    public abstract class IUnion
    {
        protected IDatabase db;
        protected string _sql;
        protected string _sqlCount;
        protected int skip;
        protected int take;
        object par;
        DistributedTransaction tran;
        int? timeout;
        public IUnion(IDatabase db)
        {
            returnFields = "*";
            this.db = db;
        }

        public IUnion Union(IQuery query)
        {
            if (db.DbType != DataBaseType.MySql && db.DbType != DataBaseType.Postgresql && db.DbType != DataBaseType.ClickHouse)
            {
                query.sqlOrderBy = null;
            }
            if (string.IsNullOrEmpty(_sql))
            {

                if (db.DbType != DataBaseType.Sqlite)
                {
                    _sql += $"({query.GetSql()})";
                }
                else
                {
                    _sql += query.GetSql();
                }
            }
            else
            {
                if (db.DbType != DataBaseType.Sqlite)
                {
                    _sql += $" UNION ({query.GetSql()})";
                }
                else
                {
                    _sql += $" UNION {query.GetSql()}";
                }
            }
            return this;
        }

        public IUnion UnionAll(IQuery query)
        {
            if (db.DbType != DataBaseType.MySql && db.DbType != DataBaseType.Postgresql && db.DbType != DataBaseType.ClickHouse)
            {
                query.sqlOrderBy = null;
            }

            if (string.IsNullOrEmpty(_sql))
            {
                if (db.DbType != DataBaseType.Sqlite)
                {
                    _sql += $"({query.GetSql()})";
                }
                else
                {
                    _sql += query.GetSql();
                }
            }
            else
            {
                if (db.DbType != DataBaseType.Sqlite)
                {
                    _sql += $" UNION ALL ({query.GetSql()})";
                }
                else
                {
                    _sql += $" UNION ALL {query.GetSql()}";
                }
            }
            return this;
        }

        #region Abstract

        internal abstract void Build();

        internal abstract void BuildCount();

        #endregion

        #region Method

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

        public void Clear()
        {
            returnFields = "*";
            skip = 0;
            take = 0;
            par = null;
            tran = null;
            timeout = 0;
            _sql = null;
            _sqlCount = null;
            sqlWhere = null;
            sqlGroupBy = null;
            sqlHaving = null;
            sqlOrderBy = null;
        }

        #endregion

        #region Query Condition

        protected string sqlWhere;
        public IUnion Where(string where)
        {
            sqlWhere = $" WHERE {where}";
            return this;
        }

        protected string sqlGroupBy;
        public IUnion GroupBy(string groupBy)
        {
            sqlGroupBy = $" GROUP BY {groupBy}";
            return this;
        }

        protected string sqlHaving;
        public IUnion Having(string having)
        {
            sqlHaving = $" HAVING {having}";
            return this;
        }

        protected string sqlOrderBy;
        public IUnion OrderBy(string orderBy)
        {
            sqlOrderBy = $" ORDER BY {orderBy}";
            return this;
        }

        protected string returnFields;
        public IUnion ReturnFields(string fields)
        {
            returnFields = fields;
            return this;
        }

        public IUnion Limit(int count)
        {
            skip = 0;
            take = count;
            return this;
        }

        public IUnion Limit(int skip, int take)
        {
            this.skip = skip;
            this.take = take;
            return this;
        }

        public IUnion Page(int page, int pageSize)
        {
            int skip = 0;
            if (page > 1)
            {
                skip = (page - 1) * pageSize;
            }
            this.skip = skip;
            take = pageSize;
            return this;
        }

        public IUnion Param(object param, DistributedTransaction tran = null, int? timeout = null)
        {
            par = param;
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
