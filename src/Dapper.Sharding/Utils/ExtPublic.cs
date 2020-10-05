using System.Collections.Generic;
using System.Data;
using System.Data.SQLite;
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
            if (list == null || list.Count() == 0)
                return;
            var accessor = TypeAccessor.Create(typeof(T));
            foreach (var item in list)
            {
                var id = accessor[item, field];
                accessor[item, propertyName] = mapList.AsQueryable().FirstOrDefault(mapField + "=@0", id);
            }
        }

        public static void MapOneToMany<T, T2>(this IEnumerable<T> list, string field, string propertyName, IEnumerable<T2> mapList, string mapField) where T : class where T2 : class
        {
            if (list == null || list.Count() == 0)
                return;
            var accessor = TypeAccessor.Create(typeof(T));
            foreach (var item in list)
            {
                var id = accessor[item, field];
                accessor[item, propertyName] = mapList.AsQueryable().Where(mapField + "=@0", id).ToList();
            }
        }

        public static void MapManyToMany<T, T2, T3>(this IEnumerable<T> list, string field, string propertyName, IEnumerable<T2> centerList, string prevField, string nextField, IEnumerable<T3> mapList, string mapField) where T : class where T2 : class where T3 : class
        {
            if (list == null || list.Count() == 0)
                return;
            var accessor = TypeAccessor.Create(typeof(T));
            foreach (var item in list)
            {
                var id = accessor[item, field];
                var ids = centerList.AsQueryable().Where($"{prevField}=@0", id).Select(nextField);
                accessor[item, propertyName] = mapList.AsQueryable().Where($"@0.Contains({mapField})", ids).ToList();
            }
        }

        #endregion

        #region IEnumerable map oneToOne oneToMany

        public static void MapTableOneToOne<T, T2>(this IEnumerable<T> list, string field, string propertyName, ICommon<T2> table, string mapField, string returnFields = null) where T : class where T2 : class
        {
            if (list == null || list.Count() == 0)
                return;
            var ids = list.AsQueryable().Select(field).Distinct().ToDynamicList();
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

        public static void MapTableOneToMany<T, T2>(this IEnumerable<T> list, string field, string propertyName, ICommon<T2> table, string mapField, string returnFields = null) where T : class where T2 : class
        {
            if (list == null || list.Count() == 0)
                return;
            var ids = list.AsQueryable().Select(field).Distinct().ToDynamicList();
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

        public static void MapTableOneToMany<T, T2>(this IEnumerable<T> list, string field, string propertyName, ICommon<T2> table, string mapField, string returnFields, string and, DynamicParameters par = null, string orderby = null) where T : class where T2 : class
        {
            if (list == null || list.Count() == 0)
                return;
            var ids = list.AsQueryable().Select(field).Distinct().ToDynamicList();
            string where = $"WHERE {mapField} IN @ids {and}";
            if (par == null)
                par = new DynamicParameters();
            par.Add("@ids", ids);
            var data = table.GetByWhere(where, par, returnFields, orderby);
            list.MapOneToMany(field, propertyName, data, mapField);
        }

        #endregion

        #region IEnumerable map many to many

        public static void MapTableManyToMany<T, T2, T3>(this IEnumerable<T> list, string field, string propertyName, ICommon<T2> centerTable, string prevField, string nextField, ICommon<T3> mapTable, string mapField, string returnFields = null) where T : class where T2 : class where T3 : class
        {
            if (list == null || list.Count() == 0)
                return;
            var ids = list.AsQueryable().Select(field).Distinct().ToDynamicList();
            IEnumerable<T2> data;
            if (ids.Count() > 1)
            {
                data = centerTable.GetByIdsWithField(ids, prevField, returnFields);
            }
            else
            {
                data = centerTable.GetByWhere($"WHERE {prevField}=@id", new { id = ids.FirstOrDefault() });
            }

            var ids2 = data.AsQueryable().Select(nextField).Distinct().AsEnumerable();
            IEnumerable<T3> data2;
            if (nextField.ToLower() == mapTable.SqlField.PrimaryKey.ToLower()) //主键
            {
                if (ids2.Count() > 1)
                {
                    data2 = mapTable.GetByIds(ids2, returnFields);
                }
                else
                {
                    data2 = new List<T3> { mapTable.GetById(ids2.FirstOrDefault(), returnFields) };
                }
            }
            else
            {
                if (ids2.Count() > 1)
                {
                    data2 = mapTable.GetByIdsWithField(ids2, mapField, returnFields);
                }
                else
                {
                    data2 = mapTable.GetByWhere($"WHERE {mapField}=@id", new { id = ids2.FirstOrDefault() }, returnFields);
                }
            }


            list.MapManyToMany(field, propertyName, data, prevField, nextField, data2, mapField);

        }

        public static void MapTableManyToMany<T, T2, T3>(this IEnumerable<T> list, string field, string propertyName, ICommon<T2> centerTable, string prevField, string nextField, ICommon<T3> mapTable, string mapField, string returnFields, string and, DynamicParameters par = null, string orderby = null) where T : class where T2 : class where T3 : class
        {
            if (list == null || list.Count() == 0)
                return;
            var ids = list.AsQueryable().Select(field).Distinct().ToDynamicList();
            IEnumerable<T2> data;
            if (ids.Count() > 1)
            {
                data = centerTable.GetByIdsWithField(ids, prevField, returnFields);
            }
            else
            {
                data = centerTable.GetByWhere($"WHERE {prevField}=@id", new { id = ids.FirstOrDefault() });
            }
            var ids2 = data.AsQueryable().Select(nextField).Distinct().ToDynamicList();

            string where = $"WHERE {mapField} IN @ids {and}";
            if (par == null)
                par = new DynamicParameters();
            par.Add("@ids", ids2);
            var data2 = mapTable.GetByWhere(where, par, returnFields, orderby);
            list.MapManyToMany(field, propertyName, data, prevField, nextField, data2, mapField);

        }


        #endregion

        #region ITable<T> map many to many

        public static IEnumerable<T> MapCenterTable<T, T2>(this ITable<T> table, ITable<T2> centerTable, string prevField, string nextField, object id, string returnFields = null) where T : class where T2 : class
        {
            var where = $"AS A WHERE EXISTS(SELECT 1 FROM {centerTable.Name} WHERE {prevField}=A.{table.SqlField.PrimaryKey} AND {nextField}=@id)";
            return table.GetByWhere(where, new { id }, returnFields);
        }

        public static IEnumerable<T> MapCenterTable<T, T2>(this ITable<T> table, ITable<T2> centerTable, string prevField, string nextField, object id, string returnFields, string and, DynamicParameters par = null, string orderby = null, int limit = 0) where T : class where T2 : class
        {
            if (par == null)
                par = new DynamicParameters();
            par.Add("@nextidddd", id);
            var where = $"AS A WHERE EXISTS(SELECT 1 FROM {centerTable.Name} WHERE {prevField}=A.{table.SqlField.PrimaryKey} AND {nextField}=@nextidddd) {and}";
            return table.GetByWhere(where, par, returnFields, orderby, limit);
        }

        public static IEnumerable<T> MapCenterTable<T, T2>(this ITable<T> table, ITable<T2> centerTable, string prevField, string nextField, object id, string returnFields, int page, int pageSize, out long total, string and = null, DynamicParameters par = null, string orderby = null) where T : class where T2 : class
        {
            if (par == null)
                par = new DynamicParameters();
            par.Add("@nextidddd", id);
            var where = $"AS A WHERE EXISTS(SELECT 1 FROM {centerTable.Name} WHERE {prevField}=A.{table.SqlField.PrimaryKey} AND {nextField}=@nextidddd) {and}";
            return table.GetByPageAndCount(page, pageSize, out total, where, par, returnFields, orderby);
        }

        #endregion
    }
}
