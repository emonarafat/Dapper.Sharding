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

        #region IEnumerable map base

        public static void MapOneToOne<T, T2>(this IEnumerable<T> list, string field, string propertyName, IEnumerable<T2> mapList, string mapField) where T : class where T2 : class
        {
            var accessor = TypeAccessor.Create(typeof(T));
            foreach (var item in list)
            {
                var id = accessor[item, field];
                accessor[item, propertyName] = mapList.AsQueryable().FirstOrDefault(mapField + "=@0", id);
            }
        }

        public static void MapOneToMany<T, T2>(this IEnumerable<T> list, string field, string propertyName, IEnumerable<T2> mapList, string mapField) where T : class where T2 : class
        {
            var accessor = TypeAccessor.Create(typeof(T));
            foreach (var item in list)
            {
                var id = accessor[item, field];
                accessor[item, propertyName] = mapList.AsQueryable().Where(mapField + "=@0", id).ToList();
            }
        }

        public static void MapManyToMany<T, T2, T3>(this IEnumerable<T> list, string field, string propertyName, IEnumerable<T2> centerList, string prevField, string nextField, IEnumerable<T3> mapList, string mapField) where T : class where T2 : class where T3 : class
        {
            var accessor = TypeAccessor.Create(typeof(T));
            foreach (var item in list)
            {
                var id = accessor[item, field];
                var ids = centerList.AsQueryable().Where($"{prevField}=@0", id).Select(nextField).AsEnumerable();
                accessor[item, propertyName] = mapList.AsQueryable().Where($"{mapField}.Any(@0)", ids).AsEnumerable<T3>();
            }
        }

        #endregion

        #region IEnumerable oneToOne oneToMany

        public static void MapTableOneToOne<T, T2>(this IEnumerable<T> list, string field, string propertyName, ITable<T2> table, string mapField, string returnFields = null) where T : class where T2 : class
        {
            var ids = list.AsQueryable().Select(field).Distinct().AsEnumerable();
            IEnumerable<T2> data;
            if (mapField.ToLower() == table.SqlField.PrimaryKey.ToLower())
            {
                if (ids.Count() > 1)
                {
                    data = table.GetByIds(ids, returnFields);
                }
                else
                {
                    data = new List<T2> { table.GetById(ids.FirstOrDefault(), returnFields) };
                }

            }
            else
            {
                if (ids.Count() > 1)
                {
                    data = table.GetByIdsWithField(ids, mapField, returnFields);
                }
                else
                {
                    data = table.GetByWhere($"WHERE {mapField}=@id", new { id = ids.FirstOrDefault() }, returnFields);
                }

            }
            list.MapOneToOne(field, propertyName, data, mapField);
        }

        public static void MapTableOneToMany<T, T2>(this IEnumerable<T> list, string field, string propertyName, ITable<T2> table, string mapField, string returnFields = null) where T : class where T2 : class
        {
            var ids = list.AsQueryable().Select(field).Distinct().AsEnumerable();
            IEnumerable<T2> data;
            if (mapField.ToLower() == table.SqlField.PrimaryKey.ToLower())
            {
                if (ids.Count() > 1)
                {
                    data = table.GetByIds(ids, returnFields);
                }
                else
                {
                    data = new List<T2> { table.GetById(ids.FirstOrDefault(), returnFields) };
                }

            }
            else
            {
                if (ids.Count() > 1)
                {
                    data = table.GetByIdsWithField(ids, mapField, returnFields);
                }
                else
                {
                    data = table.GetByWhere($"WHERE {mapField}=@id", new { id = ids.FirstOrDefault() }, returnFields);
                }

            }
            list.MapOneToMany(field, propertyName, data, mapField);
        }

        public static void MapTableOneToOne<T, T2>(this IEnumerable<T> list, string field, string propertyName, ShardingQuery<T2> table, string mapField, string returnFields = null) where T : class where T2 : class
        {
            var ids = list.AsQueryable().Select(field).Distinct().AsEnumerable();
            IEnumerable<T2> data;
            if (mapField.ToLower() == table.SqlField.PrimaryKey.ToLower())
            {
                if (ids.Count() > 1)
                {
                    data = table.GetByIds(ids, returnFields);
                }
                else
                {
                    data = new List<T2> { table.GetById(ids.FirstOrDefault(), returnFields) };
                }

            }
            else
            {
                if (ids.Count() > 1)
                {
                    data = table.GetByIdsWithField(ids, mapField, returnFields);
                }
                else
                {
                    data = table.GetByWhere($"WHERE {mapField}=@id", new { id = ids.FirstOrDefault() }, returnFields);
                }

            }
            list.MapOneToOne(field, propertyName, data, mapField);
        }

        public static void MapTableOneToMany<T, T2>(this IEnumerable<T> list, string field, string propertyName, ShardingQuery<T2> table, string mapField, string returnFields = null) where T : class where T2 : class
        {
            var ids = list.AsQueryable().Select(field).Distinct().AsEnumerable();
            IEnumerable<T2> data;
            if (mapField.ToLower() == table.SqlField.PrimaryKey.ToLower())
            {
                if (ids.Count() > 1)
                {
                    data = table.GetByIds(ids, returnFields);
                }
                else
                {
                    data = new List<T2> { table.GetById(ids.FirstOrDefault(), returnFields) };
                }

            }
            else
            {
                if (ids.Count() > 1)
                {
                    data = table.GetByIdsWithField(ids, mapField, returnFields);
                }
                else
                {
                    data = table.GetByWhere($"WHERE {mapField}=@id", new { id = ids.FirstOrDefault() }, returnFields);
                }

            }
            list.MapOneToMany(field, propertyName, data, mapField);
        }

        public static void MapTableOneToOne<T, T2>(this IEnumerable<T> list, string field, string propertyName, ISharding<T2> table, string mapField, string returnFields = null) where T : class where T2 : class
        {
            var ids = list.AsQueryable().Select(field).Distinct().AsEnumerable();
            IEnumerable<T2> data;
            if (mapField.ToLower() == table.KeyName.ToLower())
            {
                if (ids.Count() > 1)
                {
                    data = table.GetByIds(ids, returnFields);
                }
                else
                {
                    data = new List<T2> { table.GetById(ids.FirstOrDefault(), returnFields) };
                }

            }
            else
            {
                if (ids.Count() > 1)
                {
                    data = table.Query.GetByIdsWithField(ids, mapField, returnFields);
                }
                else
                {
                    data = table.Query.GetByWhere($"WHERE {mapField}=@id", new { id = ids.FirstOrDefault() }, returnFields);
                }

            }
            list.MapOneToOne(field, propertyName, data, mapField);
        }

        public static void MapTableOneToMany<T, T2>(this IEnumerable<T> list, string field, string propertyName, ISharding<T2> table, string mapField, string returnFields = null) where T : class where T2 : class
        {
            var ids = list.AsQueryable().Select(field).Distinct().AsEnumerable();
            IEnumerable<T2> data;
            if (mapField.ToLower() == table.KeyName.ToLower())
            {
                if (ids.Count() > 1)
                {
                    data = table.GetByIds(ids, returnFields);
                }
                else
                {
                    data = new List<T2> { table.GetById(ids.FirstOrDefault(), returnFields) };
                }

            }
            else
            {
                if (ids.Count() > 1)
                {
                    data = table.Query.GetByIdsWithField(ids, mapField, returnFields);
                }
                else
                {
                    data = table.Query.GetByWhere($"WHERE {mapField}=@id", new { id = ids.FirstOrDefault() }, returnFields);
                }

            }
            list.MapOneToMany(field, propertyName, data, mapField);
        }

        #endregion
    }
}
