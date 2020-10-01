using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Dynamic.Core;

namespace Dapper.Sharding
{
    public static class ExtPublic
    {
        #region dapper

        public static DataTable GetDataTable(this IDbConnection conn, string sql, object param = null, IDbTransaction tran = null, int? commandTimeout = null, CommandType? commandType = null)
        {
            if (conn.State == ConnectionState.Closed)
                conn.Open();

            using (IDataReader reader = conn.ExecuteReader(sql, param, tran, commandTimeout, commandType))
            {
                DataTable dt = new DataTable();
                dt.Load(reader);
                return dt;
            }
        }

        public static DataSet GetDataSet(this IDbConnection conn, string sql, object param = null, IDbTransaction tran = null, int? commandTimeout = null, CommandType? commandType = null)
        {
            //oracle do no support GetDataSet

            if (conn.State == ConnectionState.Closed)
                conn.Open();
            using (IDataReader reader = conn.ExecuteReader(sql, param, tran, commandTimeout, commandType))
            {
                DataSet ds = new DataSet();
                int i = 0;
                while (!reader.IsClosed)
                {
                    i++;
                    DataTable dt = new DataTable();
                    dt.TableName = "T" + i;
                    dt.Load(reader);
                    ds.Tables.Add(dt);
                }
                return ds;
            }
        }

        #endregion

        #region IEnumerable

        public static IEnumerable<T> TableMapOneToOne<T, Tb>(this IEnumerable<T> list, string field, string propertyName, ITable<Tb> table, string returnFields = null) where Tb : class
        {
            var ids = list.AsQueryable().Select(field).Distinct().AsEnumerable();
            var data = table.GetByIds(ids, returnFields);
            var accessor = TypeAccessor.Create(typeof(T));
            var where = table.SqlField.PrimaryKey + "=@0";
            foreach (var item in list)
            {
                var id = accessor[item, field];
                accessor[item, propertyName] = data.AsQueryable().FirstOrDefault(where, id);
            }
            return list;
        }

        public static IEnumerable<T> TableMapOneToMany<T, Tb>(this IEnumerable<T> list, string field, string propertyName, ITable<Tb> table, string returnFields = null) where Tb : class
        {
            var ids = list.AsQueryable().Select(field).Distinct().AsEnumerable();
            var data = table.GetByIds(ids, returnFields);
            var accessor = TypeAccessor.Create(typeof(T));
            var where = table.SqlField.PrimaryKey + "=@0";
            foreach (var item in list)
            {
                var id = accessor[item, field];
                accessor[item, propertyName] = data.AsQueryable().Where(where, id).ToList();
            }
            return list;
        }

        public static IEnumerable<T> TableMapOneToOne<T, Tb>(this IEnumerable<T> list, string field, string propertyName, ISharding<Tb> sharding, string returnFields = null) where Tb : class
        {
            var ids = list.AsQueryable().Select(field).Distinct().AsEnumerable();
            var data = sharding.GetByIds(ids, returnFields);
            var accessor = TypeAccessor.Create(typeof(T));
            var where = sharding.KeyName + "=@0";
            foreach (var item in list)
            {
                var id = accessor[item, field];
                accessor[item, propertyName] = data.AsQueryable().FirstOrDefault(where, id);
            }
            return list;
        }

        public static IEnumerable<T> TableMapOneToMany<T, Tb>(this IEnumerable<T> list, string field, string propertyName, ISharding<Tb> sharding, string returnFields = null) where Tb : class
        {
            var ids = list.AsQueryable().Select(field).Distinct().AsEnumerable();
            var data = sharding.GetByIds(ids, returnFields);
            var accessor = TypeAccessor.Create(typeof(T));
            var where = sharding.KeyName + "=@0";
            foreach (var item in list)
            {
                var id = accessor[item, field];
                accessor[item, propertyName] = data.AsQueryable().Where(where, id).ToList();
            }
            return list;
        }

        public static IEnumerable<T> TableMapOneToOne<T, Tb>(this IEnumerable<T> list, string field, string propertyName, ShardingQuery<Tb> table, string returnFields = null) where Tb : class
        {
            var ids = list.AsQueryable().Select(field).Distinct().AsEnumerable();
            var data = table.GetByIds(ids, returnFields);
            var accessor = TypeAccessor.Create(typeof(T));
            var where = table.SqlField.PrimaryKey + "=@0";
            foreach (var item in list)
            {
                var id = accessor[item, field];
                accessor[item, propertyName] = data.AsQueryable().FirstOrDefault(where, id);
            }
            return list;
        }

        public static IEnumerable<T> TableMapOneToMany<T, Tb>(this IEnumerable<T> list, string field, string propertyName, ShardingQuery<Tb> table, string returnFields = null) where Tb : class
        {
            var ids = list.AsQueryable().Select(field).Distinct().AsEnumerable();
            var data = table.GetByIds(ids, returnFields);
            var accessor = TypeAccessor.Create(typeof(T));
            var where = table.SqlField.PrimaryKey + "=@0";
            foreach (var item in list)
            {
                var id = accessor[item, field];
                accessor[item, propertyName] = data.AsQueryable().Where(where, id).ToList();
            }
            return list;
        }


        #endregion
    }
}
