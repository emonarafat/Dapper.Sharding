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

        public static void MapOneToOne<T, T2>(this IEnumerable<T> list, string field, string propertyName, IEnumerable<T2> mapList, string mapField)
        {
            var accessor = TypeAccessor.Create(typeof(T));
            foreach (var item in list)
            {
                var id = accessor[item, field];
                accessor[item, propertyName] = mapList.AsQueryable().FirstOrDefault(mapField + "=@0", id);
            }
        }

        public static void MapOneToMany<T, T2>(this IEnumerable<T> list, string field, string propertyName, IEnumerable<T2> mapList, string mapField)
        {
            var accessor = TypeAccessor.Create(typeof(T));
            foreach (var item in list)
            {
                var id = accessor[item, field];
                accessor[item, propertyName] = mapList.AsQueryable().Where(mapField + "=@0", id).ToList();
            }
        }

        public static void MapTableOneToOne<T, Tb>(this IEnumerable<T> list, string field, string propertyName, ITable<Tb> table, string mapField, string returnFields = null) where Tb : class
        {
            var ids = list.AsQueryable().Select(field).Distinct().AsEnumerable();
            IEnumerable<Tb> data;
            if (mapField.ToLower() == table.SqlField.PrimaryKey.ToLower())
            {
                data = table.GetByIds(ids, returnFields);
            }
            else
            {
                data = table.GetByIdsWithField(ids, mapField, returnFields);
            }
            list.MapOneToOne(field, propertyName, data, mapField);
        }

        public static void MapTableOneToMany<T, Tb>(this IEnumerable<T> list, string field, string propertyName, ITable<Tb> table, string mapField = null, string returnFields = null) where Tb : class
        {
            var ids = list.AsQueryable().Select(field).Distinct().AsEnumerable();
            IEnumerable<Tb> data;
            if (mapField.ToLower() == table.SqlField.PrimaryKey.ToLower())
            {
                data = table.GetByIds(ids, returnFields);
            }
            else
            {
                data = table.GetByIdsWithField(ids, mapField, returnFields);
            }
            list.MapOneToMany(field, propertyName, data, mapField);
        }

        public static void MapTableOneToOne<T, Tb>(this IEnumerable<T> list, string field, string propertyName, ShardingQuery<Tb> table, string mapField, string returnFields = null) where Tb : class
        {
            var ids = list.AsQueryable().Select(field).Distinct().AsEnumerable();
            IEnumerable<Tb> data;
            if (mapField.ToLower() == table.SqlField.PrimaryKey.ToLower())
            {
                data = table.GetByIds(ids, returnFields);
            }
            else
            {
                data = table.GetByIdsWithField(ids, mapField, returnFields);
            }
            list.MapOneToOne(field, propertyName, data, mapField);
        }

        public static void MapTableOneToMany<T, Tb>(this IEnumerable<T> list, string field, string propertyName, ShardingQuery<Tb> table, string mapField = null, string returnFields = null) where Tb : class
        {
            var ids = list.AsQueryable().Select(field).Distinct().AsEnumerable();
            IEnumerable<Tb> data;
            if (mapField.ToLower() == table.SqlField.PrimaryKey.ToLower())
            {
                data = table.GetByIds(ids, returnFields);
            }
            else
            {
                data = table.GetByIdsWithField(ids, mapField, returnFields);
            }
            list.MapOneToMany(field, propertyName, data, mapField);
        }

        public static void MapTableOneToOne<T, Tb>(this IEnumerable<T> list, string field, string propertyName, ISharding<Tb> table, string mapField, string returnFields = null) where Tb : class
        {
            var ids = list.AsQueryable().Select(field).Distinct().AsEnumerable();
            IEnumerable<Tb> data;
            if (mapField.ToLower() == table.KeyName.ToLower())
            {
                data = table.GetByIds(ids, returnFields);
            }
            else
            {
                data = table.Query.GetByIdsWithField(ids, mapField, returnFields);
            }
            list.MapOneToOne(field, propertyName, data, mapField);
        }

        public static void MapTableOneToMany<T, Tb>(this IEnumerable<T> list, string field, string propertyName, ISharding<Tb> table, string mapField = null, string returnFields = null) where Tb : class
        {
            var ids = list.AsQueryable().Select(field).Distinct().AsEnumerable();
            IEnumerable<Tb> data;
            if (mapField.ToLower() == table.KeyName.ToLower())
            {
                data = table.GetByIds(ids, returnFields);
            }
            else
            {
                data = table.Query.GetByIdsWithField(ids, mapField, returnFields);
            }
            list.MapOneToMany(field, propertyName, data, mapField);
        }

        #endregion
    }
}
