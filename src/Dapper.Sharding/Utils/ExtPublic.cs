using System;
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

            var ids = list.AsQueryable().Select(field).Distinct();
            var first = ids.First();
            Type t = first.GetType();
            IEnumerable<T2> data;
            if (mapField.ToLower() == table.SqlField.PrimaryKey.ToLower())
            {
                if (ids.Count() > 1)
                {
                    if (t == typeof(long))
                    {
                        data = table.GetByIds(ids.OfType<long>().ToList(), returnFields);
                    }
                    else if (t == typeof(string))
                    {
                        data = table.GetByIds(ids.OfType<string>().ToList(), returnFields);
                    }
                    else if (t == typeof(int))
                    {
                        data = table.GetByIds(ids.OfType<int>().ToList(), returnFields);
                    }
                    else if (t == typeof(decimal))
                    {
                        data = table.GetByIds(ids.OfType<decimal>().ToList(), returnFields);
                    }
                    else
                    {
                        data = table.GetByIds(ids.ToDynamicList(), returnFields);
                    }

                }
                else
                {
                    if (t == typeof(long))
                    {
                        data = new List<T2> { table.GetById((long)first, returnFields) };
                    }
                    else if (t == typeof(string))
                    {
                        data = new List<T2> { table.GetById((string)first, returnFields) };
                    }
                    else if (t == typeof(int))
                    {
                        data = new List<T2> { table.GetById((int)first, returnFields) };
                    }
                    else if (t == typeof(decimal))
                    {
                        data = new List<T2> { table.GetById((decimal)first, returnFields) };
                    }
                    else
                    {
                        data = new List<T2> { table.GetById(first, returnFields) };
                    }

                }

            }
            else
            {
                if (ids.Count() > 1)
                {
                    if (t == typeof(long))
                    {
                        data = table.GetByIdsWithField(ids.OfType<long>().ToList(), mapField, returnFields);
                    }
                    else if (t == typeof(string))
                    {
                        data = table.GetByIdsWithField(ids.OfType<string>().ToList(), mapField, returnFields);
                    }
                    else if (t == typeof(int))
                    {
                        data = table.GetByIdsWithField(ids.OfType<int>().ToList(), mapField, returnFields);
                    }
                    else if (t == typeof(decimal))
                    {
                        data = table.GetByIdsWithField(ids.OfType<decimal>().ToList(), mapField, returnFields);
                    }
                    else
                    {
                        data = table.GetByIdsWithField(ids.ToDynamicList(), mapField, returnFields);
                    }

                }
                else
                {
                    if (t == typeof(long))
                    {
                        data = table.GetByWhere($"WHERE {mapField}=@id", new { id = (long)first }, returnFields);
                    }
                    else if (t == typeof(string))
                    {
                        data = table.GetByWhere($"WHERE {mapField}=@id", new { id = (string)first }, returnFields);
                    }
                    else if (t == typeof(int))
                    {
                        data = table.GetByWhere($"WHERE {mapField}=@id", new { id = (int)first }, returnFields);
                    }
                    else if (t == typeof(decimal))
                    {
                        data = table.GetByWhere($"WHERE {mapField}=@id", new { id = (decimal)first }, returnFields);
                    }
                    else
                    {
                        data = table.GetByWhere($"WHERE {mapField}=@id", new { id = first }, returnFields);
                    }

                }

            }
            list.MapOneToOne(field, propertyName, data, mapField);
        }

        public static void MapTableOneToMany<T, T2>(this IEnumerable<T> list, string field, string propertyName, ICommon<T2> table, string mapField, string returnFields = null) where T : class where T2 : class
        {
            if (list == null || list.Count() == 0)
                return;
            var ids = list.AsQueryable().Select(field).Distinct();
            var first = ids.First();
            Type t = first.GetType();
            IEnumerable<T2> data;
            if (mapField.ToLower() == table.SqlField.PrimaryKey.ToLower())
            {
                if (ids.Count() > 1)
                {
                    if (t == typeof(long))
                    {
                        data = table.GetByIds(ids.OfType<long>().ToList(), returnFields);
                    }
                    else if (t == typeof(string))
                    {
                        data = table.GetByIds(ids.OfType<string>().ToList(), returnFields);
                    }
                    else if (t == typeof(int))
                    {
                        data = table.GetByIds(ids.OfType<int>().ToList(), returnFields);
                    }
                    else if (t == typeof(decimal))
                    {
                        data = table.GetByIds(ids.OfType<decimal>().ToList(), returnFields);
                    }
                    else
                    {
                        data = table.GetByIds(ids.ToDynamicList(), returnFields);
                    }

                }
                else
                {
                    if (t == typeof(long))
                    {
                        data = new List<T2> { table.GetById((long)first, returnFields) };
                    }
                    else if (t == typeof(string))
                    {
                        data = new List<T2> { table.GetById((string)first, returnFields) };
                    }
                    else if (t == typeof(int))
                    {
                        data = new List<T2> { table.GetById((int)first, returnFields) };

                    }
                    else if (t == typeof(decimal))
                    {
                        data = new List<T2> { table.GetById((decimal)first, returnFields) };
                    }
                    else
                    {
                        data = new List<T2> { table.GetById(first, returnFields) };
                    }

                }

            }
            else
            {
                if (ids.Count() > 1)
                {
                    if (t == typeof(long))
                    {
                        data = table.GetByIdsWithField(ids.OfType<long>().ToList(), mapField, returnFields);
                    }
                    else if (t == typeof(string))
                    {
                        data = table.GetByIdsWithField(ids.OfType<string>().ToList(), mapField, returnFields);
                    }
                    else if (t == typeof(int))
                    {
                        data = table.GetByIdsWithField(ids.OfType<int>().ToList(), mapField, returnFields);
                    }
                    else if (t == typeof(decimal))
                    {
                        data = table.GetByIdsWithField(ids.OfType<decimal>().ToList(), mapField, returnFields);
                    }
                    else
                    {
                        data = table.GetByIdsWithField(ids, mapField, returnFields);
                    }

                }
                else
                {
                    if (t == typeof(long))
                    {
                        data = table.GetByWhere($"WHERE {mapField}=@id", new { id = (long)first }, returnFields);
                    }
                    else if (t == typeof(string))
                    {
                        data = table.GetByWhere($"WHERE {mapField}=@id", new { id = (string)first }, returnFields);
                    }
                    else if (t == typeof(int))
                    {
                        data = table.GetByWhere($"WHERE {mapField}=@id", new { id = (int)first }, returnFields);
                    }
                    else if (t == typeof(decimal))
                    {
                        data = table.GetByWhere($"WHERE {mapField}=@id", new { id = (decimal)first }, returnFields);
                    }
                    else
                    {
                        data = table.GetByWhere($"WHERE {mapField}=@id", new { id = first }, returnFields);
                    }

                }

            }
            list.MapOneToMany(field, propertyName, data, mapField);
        }

        public static void MapTableOneToMany<T, T2>(this IEnumerable<T> list, string field, string propertyName, ICommon<T2> table, string mapField, string returnFields, string and, DynamicParameters par = null, string orderby = null) where T : class where T2 : class
        {
            if (list == null || list.Count() == 0)
                return;
            var ids = list.AsQueryable().Select(field).Distinct();
            var t = ids.First().GetType();
            if (par == null)
                par = new DynamicParameters();
            if (t == typeof(long))
            {
                par.Add("@ids", ids.OfType<long>().ToList());
            }
            else if (t == typeof(string))
            {
                par.Add("@ids", ids.OfType<string>().ToList());
            }
            else if (t == typeof(int))
            {
                par.Add("@ids", ids.OfType<int>().ToList());
            }
            else if (t == typeof(decimal))
            {
                par.Add("@ids", ids.OfType<decimal>().ToList());
            }
            else
            {
                par.Add("@ids", ids.ToDynamicList());
            }
            string where;
            if (table.DataBase.Client.DbType == DataBaseType.Postgresql)
            {
                where = $"WHERE {mapField}=ANY(@ids) {and}";
            }
            else 
            {
                where = $"WHERE {mapField} IN @ids {and}";
            }
            var data = table.GetByWhere(where, par, returnFields, orderby);
            list.MapOneToMany(field, propertyName, data, mapField);
        }

        #endregion

        #region IEnumerable map many to many

        public static void MapTableManyToMany<T, T2, T3>(this IEnumerable<T> list, string field, string propertyName, ICommon<T2> centerTable, string prevField, string nextField, ICommon<T3> mapTable, string mapField, string returnFields = null) where T : class where T2 : class where T3 : class
        {
            if (list == null || list.Count() == 0)
                return;
            var ids = list.AsQueryable().Select(field).Distinct();
            var first = ids.First();
            Type t = first.GetType();
            IEnumerable<T2> data;
            if (ids.Count() > 1)
            {
                if (t == typeof(long))
                {
                    data = centerTable.GetByIdsWithField(ids.OfType<long>().ToList(), prevField, returnFields);
                }
                else if (t == typeof(string))
                {
                    data = centerTable.GetByIdsWithField(ids.OfType<string>().ToList(), prevField, returnFields);
                }
                else if (t == typeof(int))
                {
                    data = centerTable.GetByIdsWithField(ids.OfType<int>().ToList(), prevField, returnFields);
                }
                else if (t == typeof(decimal))
                {
                    data = centerTable.GetByIdsWithField(ids.OfType<decimal>().ToList(), prevField, returnFields);
                }
                else
                {
                    data = centerTable.GetByIdsWithField(ids.ToDynamicList(), prevField, returnFields);
                }

            }
            else
            {
                if (t == typeof(long))
                {
                    data = centerTable.GetByWhere($"WHERE {prevField}=@id", new { id = (long)first });
                }
                else if (t == typeof(string))
                {
                    data = centerTable.GetByWhere($"WHERE {prevField}=@id", new { id = (string)first });

                }
                else if (t == typeof(int))
                {
                    data = centerTable.GetByWhere($"WHERE {prevField}=@id", new { id = (int)first });
                }
                else if (t == typeof(decimal))
                {
                    data = centerTable.GetByWhere($"WHERE {prevField}=@id", new { id = (decimal)first });
                }
                else
                {
                    data = centerTable.GetByWhere($"WHERE {prevField}=@id", new { id = first });
                }

            }

            var ids2 = data.AsQueryable().Select(nextField).Distinct();
            IEnumerable<T3> data2;
            if (ids2.Count() > 0)
            {
                var first2 = ids2.First();
                Type t2 = first2.GetType();
                if (nextField.ToLower() == mapTable.SqlField.PrimaryKey.ToLower()) //主键
                {
                    if (ids2.Count() > 1)
                    {
                        if (t2 == typeof(long))
                        {
                            data2 = mapTable.GetByIds(ids2.OfType<long>().ToList(), returnFields);
                        }
                        else if (t2 == typeof(string))
                        {
                            data2 = mapTable.GetByIds(ids2.OfType<string>().ToList(), returnFields);
                        }
                        else if (t2 == typeof(int))
                        {
                            data2 = mapTable.GetByIds(ids2.OfType<int>().ToList(), returnFields);
                        }
                        else if (t2 == typeof(decimal))
                        {
                            data2 = mapTable.GetByIds(ids2.OfType<decimal>().ToList(), returnFields);
                        }
                        else
                        {
                            data2 = mapTable.GetByIds(ids2.ToDynamicList(), returnFields);
                        }

                    }
                    else
                    {
                        if (t2 == typeof(long))
                        {
                            data2 = new List<T3> { mapTable.GetById((long)first2, returnFields) };
                        }
                        else if (t2 == typeof(string))
                        {
                            data2 = new List<T3> { mapTable.GetById((string)first2, returnFields) };
                        }
                        else if (t2 == typeof(int))
                        {
                            data2 = new List<T3> { mapTable.GetById((int)first2, returnFields) };
                        }
                        else if (t2 == typeof(decimal))
                        {
                            data2 = new List<T3> { mapTable.GetById((decimal)first2, returnFields) };
                        }
                        else
                        {
                            data2 = new List<T3> { mapTable.GetById(first2, returnFields) };
                        }

                    }
                }
                else
                {
                    if (ids2.Count() == 1)
                    {
                        if (t2 == typeof(long))
                        {
                            data2 = mapTable.GetByIdsWithField(ids2.OfType<long>().ToList(), mapField, returnFields);
                        }
                        else if (t2 == typeof(string))
                        {
                            data2 = mapTable.GetByIdsWithField(ids2.OfType<string>().ToList(), mapField, returnFields);
                        }
                        else if (t2 == typeof(int))
                        {
                            data2 = mapTable.GetByIdsWithField(ids2.OfType<int>().ToList(), mapField, returnFields);
                        }
                        else if (t2 == typeof(decimal))
                        {
                            data2 = mapTable.GetByIdsWithField(ids2.OfType<decimal>().ToList(), mapField, returnFields);
                        }
                        else
                        {
                            data2 = mapTable.GetByIdsWithField(ids2.ToDynamicList(), mapField, returnFields);
                        }

                    }
                    else
                    {
                        if (t2 == typeof(long))
                        {
                            data2 = mapTable.GetByWhere($"WHERE {mapField}=@id", new { id = (long)first2 }, returnFields);
                        }
                        else if (t2 == typeof(string))
                        {
                            data2 = mapTable.GetByWhere($"WHERE {mapField}=@id", new { id = (string)first2 }, returnFields);
                        }
                        else if (t2 == typeof(int))
                        {
                            data2 = mapTable.GetByWhere($"WHERE {mapField}=@id", new { id = (int)first2 }, returnFields);
                        }
                        else if (t2 == typeof(decimal))
                        {
                            data2 = mapTable.GetByWhere($"WHERE {mapField}=@id", new { id = (decimal)first2 }, returnFields);
                        }
                        else
                        {
                            data2 = mapTable.GetByWhere($"WHERE {mapField}=@id", new { id = first2 }, returnFields);
                        }

                    }
                }
            }
            else
            {
                data2 = Enumerable.Empty<T3>();
            }


            list.MapManyToMany(field, propertyName, data, prevField, nextField, data2, mapField);

        }

        public static void MapTableManyToMany<T, T2, T3>(this IEnumerable<T> list, string field, string propertyName, ICommon<T2> centerTable, string prevField, string nextField, ICommon<T3> mapTable, string mapField, string returnFields, string and, DynamicParameters par = null, string orderby = null) where T : class where T2 : class where T3 : class
        {
            if (list == null || list.Count() == 0)
                return;
            var ids = list.AsQueryable().Select(field).Distinct();
            var first = ids.First();
            Type t = first.GetType();
            IEnumerable<T2> data;
            if (ids.Count() > 1)
            {
                if (t == typeof(long))
                {
                    data = centerTable.GetByIdsWithField(ids.OfType<long>().ToList(), prevField, returnFields);
                }
                else if (t == typeof(string))
                {
                    data = centerTable.GetByIdsWithField(ids.OfType<string>().ToList(), prevField, returnFields);
                }
                else if (t == typeof(int))
                {
                    data = centerTable.GetByIdsWithField(ids.OfType<int>().ToList(), prevField, returnFields);
                }
                else if (t == typeof(decimal))
                {
                    data = centerTable.GetByIdsWithField(ids.OfType<decimal>().ToList(), prevField, returnFields);
                }
                else
                {
                    data = centerTable.GetByIdsWithField(ids.ToDynamicList(), prevField, returnFields);
                }

            }
            else
            {
                if (t == typeof(long))
                {
                    data = centerTable.GetByWhere($"WHERE {prevField}=@id", new { id = (long)first });
                }
                else if (t == typeof(string))
                {
                    data = centerTable.GetByWhere($"WHERE {prevField}=@id", new { id = (string)first });
                }
                else if (t == typeof(int))
                {
                    data = centerTable.GetByWhere($"WHERE {prevField}=@id", new { id = (int)first });
                }
                else if (t == typeof(decimal))
                {
                    data = centerTable.GetByWhere($"WHERE {prevField}=@id", new { id = (decimal)first });
                }
                else
                {
                    data = centerTable.GetByWhere($"WHERE {prevField}=@id", new { id = first });
                }

            }

            var ids2 = data.AsQueryable().Select(nextField).Distinct();
            IEnumerable<T3> data2;
            if (ids2.Count() > 0)
            {
                Type t2 = ids2.First().GetType();
                if (par == null)
                    par = new DynamicParameters();

                if (t2 == typeof(long))
                {
                    par.Add("@ids", ids2.OfType<long>().ToList());
                }
                else if (t2 == typeof(string))
                {
                    par.Add("@ids", ids2.OfType<string>().ToList());
                }
                else if (t2 == typeof(int))
                {
                    par.Add("@ids", ids2.OfType<int>().ToList());
                }
                else if (t2 == typeof(decimal))
                {
                    par.Add("@ids", ids2.OfType<decimal>().ToList());
                }
                else
                {
                    par.Add("@ids", ids2.ToDynamicList());
                }

                string where;
                if (mapTable.DataBase.Client.DbType == DataBaseType.Postgresql)
                {
                    where = $"WHERE {mapField}=ANY(@ids) {and}";
                }
                else
                {
                    where = $"WHERE {mapField} IN @ids {and}";
                }
                data2 = mapTable.GetByWhere(where, par, returnFields, orderby);
            }
            else
            {
                data2 = Enumerable.Empty<T3>();
            }

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
