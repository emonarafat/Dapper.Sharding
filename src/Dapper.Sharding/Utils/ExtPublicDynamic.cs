using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Dynamic.Core;
using System.Linq.Dynamic.Core.CustomTypeProviders;

namespace Dapper.Sharding
{
    #region linq.dynamic.core Extending

    [DynamicLinqType]
    public static class ExtUtils
    {
        public static int Int(object val)
        {
            return (int)val;
        }

        public static long Long(object val)
        {
            return (long)val;
        }

        public static decimal Decimal(object val)
        {
            return (decimal)val;
        }

        public static string String(object val)
        {
            return (string)val;
        }
    }

    #endregion

    public static class ExtPublicDynamic
    {
        #region IEnumerable map base

        private static void MapOneToOneDynamic<T>(this IEnumerable<T> list, string field, string propertyName, IEnumerable<dynamic> mapList, string mapField) where T : class
        {
            if (list == null || list.Count() == 0)
                return;
            if (mapList == null || mapList.Count() == 0)
                return;
            var accessor = TypeAccessor.Create(typeof(T));
            var queryable = mapList.AsQueryable();
            foreach (var item in list)
            {
                var id = accessor[item, field];
                if (id == null)
                {
                    continue;
                }

                if (id is int)
                {
                    accessor[item, propertyName] = queryable.FirstOrDefault($"ExtUtils.Int({mapField})=@0", id);
                }
                else if (id is long)
                {
                    accessor[item, propertyName] = queryable.FirstOrDefault($"ExtUtils.Long({mapField})=@0", id);
                }
                else if (id is decimal)
                {
                    accessor[item, propertyName] = queryable.FirstOrDefault($"ExtUtils.Decimal({mapField})=@0", id);
                }
                else if (id is string)
                {
                    accessor[item, propertyName] = queryable.FirstOrDefault($"ExtUtils.String({mapField})=@0", id);
                }
                else
                {
                    accessor[item, propertyName] = queryable.FirstOrDefault($"{mapField}.ToString()=@0", id.ToString());
                }

            }
        }

        private static void MapOneToManyDynamic<T>(this IEnumerable<T> list, string field, string propertyName, IEnumerable<dynamic> mapList, string mapField) where T : class
        {
            if (list == null || list.Count() == 0)
                return;
            if (mapList == null || mapList.Count() == 0)
                return;
            var accessor = TypeAccessor.Create(typeof(T));
            var queryable = mapList.AsQueryable();
            foreach (var item in list)
            {
                var id = accessor[item, field];
                if (id == null)
                {
                    continue;
                }

                if (id is int)
                {
                    accessor[item, propertyName] = queryable.Where($"ExtUtils.Int({mapField})=@0", id).ToList();
                }
                else if (id is long)
                {
                    accessor[item, propertyName] = queryable.Where($"ExtUtils.Long({mapField})=@0", id).ToList();
                }
                else if (id is decimal)
                {
                    accessor[item, propertyName] = queryable.Where($"ExtUtils.Decimal({mapField})=@0", id).ToList();
                }
                else if (id is string)
                {
                    accessor[item, propertyName] = queryable.Where($"ExtUtils.String({mapField})=@0", id).ToList();
                }
                else
                {
                    accessor[item, propertyName] = queryable.Where($"{mapField}.ToString()=@0", id.ToString()).ToList();
                }
            }
        }

        private static void MapManyToManyDynamic<T, T2>(this IEnumerable<T> list, string field, string propertyName, IEnumerable<T2> centerList, string prevField, string nextField, IEnumerable<dynamic> mapList, string mapField) where T : class where T2 : class
        {
            if (list == null || list.Count() == 0)
                return;
            if (mapList == null || mapList.Count() == 0)
                return;
            var accessor = TypeAccessor.Create(typeof(T));
            var queryable = mapList.AsQueryable();
            var centerqueryable = centerList.AsQueryable();
            foreach (var item in list)
            {
                var id = accessor[item, field];
                if (id == null)
                {
                    continue;
                }
                var ids = centerqueryable.Where($"{prevField}=@0", id).Select(nextField);
                if (ids.Any())
                {
                    var first = ids.FirstOrDefault();
                    if (first is int)
                    {
                        accessor[item, propertyName] = queryable.Where($"@0.Contains(ExtUtils.Int({mapField}))", ids).ToList();
                    }
                    else if (first is long)
                    {
                        accessor[item, propertyName] = queryable.Where($"@0.Contains(ExtUtils.Long({mapField}))", ids).ToList();
                    }
                    else if (first is decimal)
                    {
                        accessor[item, propertyName] = queryable.Where($"@0.Contains(ExtUtils.Decimal({mapField}))", ids).ToList();
                    }
                    else if (first is string)
                    {
                        accessor[item, propertyName] = queryable.Where($"@0.Contains(ExtUtils.String({mapField}))", ids).ToList();
                    }
                    else
                    {
                        var idss = ids.Select("new(it.ToString())");
                        accessor[item, propertyName] = queryable.Where($"@0.Contains({mapField}.ToString())", idss).ToList();
                    }

                }

            }
        }

        #endregion

        #region IEnumerable map oneToOne oneToMany

        private static IEnumerable<dynamic> Com<T, T2>(IEnumerable<T> list, string field, ITable<T2> table, string mapField, string returnFields = null) where T : class where T2 : class
        {
            if (list == null || list.Count() == 0)
                return Enumerable.Empty<dynamic>();
            var ids = list.AsQueryable().Where($"{field}!=null").Select(field).Distinct();
            var idsCount = ids.Count();
            if (idsCount == 0)
            {
                return Enumerable.Empty<dynamic>();
            }
            var first = ids.First();
            Type t = first.GetType();
            IEnumerable<dynamic> data;
            if (mapField.ToLower() == table.SqlField.PrimaryKey.ToLower())
            {
                if (idsCount > 1)
                {
                    if (t == typeof(long))
                    {
                        data = table.GetByIdsDynamic(ids.OfType<long>().ToList(), returnFields);
                    }
                    else if (t == typeof(string))
                    {
                        data = table.GetByIdsDynamic(ids.OfType<string>().ToList(), returnFields);
                    }
                    else if (t == typeof(int))
                    {
                        data = table.GetByIdsDynamic(ids.OfType<int>().ToList(), returnFields);
                    }
                    else if (t == typeof(decimal))
                    {
                        data = table.GetByIdsDynamic(ids.OfType<decimal>().ToList(), returnFields);
                    }
                    else
                    {
                        data = table.GetByIdsDynamic(ids.ToDynamicList(), returnFields);
                    }

                }
                else
                {
                    if (t == typeof(long))
                    {
                        data = new List<dynamic> { table.GetByIdDynamic((long)first, returnFields) };
                    }
                    else if (t == typeof(string))
                    {
                        data = new List<dynamic> { table.GetByIdDynamic((string)first, returnFields) };
                    }
                    else if (t == typeof(int))
                    {
                        data = new List<dynamic> { table.GetByIdDynamic((int)first, returnFields) };
                    }
                    else if (t == typeof(decimal))
                    {
                        data = new List<dynamic> { table.GetByIdDynamic((decimal)first, returnFields) };
                    }
                    else
                    {
                        data = new List<dynamic> { table.GetByIdDynamic(first, returnFields) };
                    }

                }

            }
            else
            {
                if (idsCount > 1)
                {
                    if (t == typeof(long))
                    {
                        data = table.GetByIdsWithFieldDynamic(ids.OfType<long>().ToList(), mapField, returnFields);
                    }
                    else if (t == typeof(string))
                    {
                        data = table.GetByIdsWithFieldDynamic(ids.OfType<string>().ToList(), mapField, returnFields);
                    }
                    else if (t == typeof(int))
                    {
                        data = table.GetByIdsWithFieldDynamic(ids.OfType<int>().ToList(), mapField, returnFields);
                    }
                    else if (t == typeof(decimal))
                    {
                        data = table.GetByIdsWithFieldDynamic(ids.OfType<decimal>().ToList(), mapField, returnFields);
                    }
                    else
                    {
                        data = table.GetByIdsWithFieldDynamic(ids.ToDynamicList(), mapField, returnFields);
                    }

                }
                else
                {
                    if (t == typeof(long))
                    {
                        data = table.GetByWhereDynamic($"WHERE {mapField}=@id", new { id = (long)first }, returnFields);
                    }
                    else if (t == typeof(string))
                    {
                        data = table.GetByWhereDynamic($"WHERE {mapField}=@id", new { id = (string)first }, returnFields);
                    }
                    else if (t == typeof(int))
                    {
                        data = table.GetByWhereDynamic($"WHERE {mapField}=@id", new { id = (int)first }, returnFields);
                    }
                    else if (t == typeof(decimal))
                    {
                        data = table.GetByWhereDynamic($"WHERE {mapField}=@id", new { id = (decimal)first }, returnFields);
                    }
                    else
                    {
                        data = table.GetByWhereDynamic($"WHERE {mapField}=@id", new { id = first }, returnFields);
                    }

                }

            }
            return data;
        }

        public static void MapTableOneToOneDynamic<T, T2>(this IEnumerable<T> list, string field, string propertyName, ITable<T2> table, string mapField, string returnFields = null) where T : class where T2 : class
        {
            var data = Com(list, field, table, mapField, returnFields);
            list.MapOneToOneDynamic(field, propertyName, data, mapField);
        }

        public static void MapTableOneToManyDynamic<T, T2>(this IEnumerable<T> list, string field, string propertyName, ITable<T2> table, string mapField, string returnFields = null) where T : class where T2 : class
        {
            var data = Com(list, field, table, mapField, returnFields);
            list.MapOneToManyDynamic(field, propertyName, data, mapField);
        }

        public static void MapTableOneToManyDynamic<T, T2>(this IEnumerable<T> list, string field, string propertyName, ITable<T2> table, string mapField, string returnFields, string and, DynamicParameters par = null, string orderby = null) where T : class where T2 : class
        {
            if (list == null || list.Count() == 0)
                return;
            var ids = list.AsQueryable().Where($"{field}!=null").Select(field).Distinct();
            var idsCount = ids.Count();
            if (idsCount == 0)
            {
                return;
            };
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
            var data = table.GetByWhereDynamic(where, par, returnFields, orderby);
            list.MapOneToManyDynamic(field, propertyName, data, mapField);
        }

        #endregion

        #region IEnumerable map many to many

        public static void MapTableManyToManyDynamic<T, T2, T3>(this IEnumerable<T> list, string field, string propertyName, ITable<T2> centerTable, string prevField, string nextField, ITable<T3> mapTable, string mapField, string returnFields = null) where T : class where T2 : class where T3 : class
        {
            if (list == null || list.Count() == 0)
                return;
            var ids = list.AsQueryable().Where($"{field}!=null").Select(field).Distinct();
            var idsCount = ids.Count();
            if (idsCount == 0)
            {
                return;
            }
            var first = ids.First();
            Type t = first.GetType();
            IEnumerable<T2> data;
            if (idsCount > 1)
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

            var ids2 = data.AsQueryable().Where($"{nextField}!=null").Select(nextField).Distinct();
            var ids2Count = ids2.Count();
            IEnumerable<dynamic> data2;
            if (ids2Count > 0)
            {
                var first2 = ids2.First();
                Type t2 = first2.GetType();
                if (nextField.ToLower() == mapTable.SqlField.PrimaryKey.ToLower()) //主键
                {
                    if (ids2Count > 1)
                    {
                        if (t2 == typeof(long))
                        {
                            data2 = mapTable.GetByIdsDynamic(ids2.OfType<long>().ToList(), returnFields);
                        }
                        else if (t2 == typeof(string))
                        {
                            data2 = mapTable.GetByIdsDynamic(ids2.OfType<string>().ToList(), returnFields);
                        }
                        else if (t2 == typeof(int))
                        {
                            data2 = mapTable.GetByIdsDynamic(ids2.OfType<int>().ToList(), returnFields);
                        }
                        else if (t2 == typeof(decimal))
                        {
                            data2 = mapTable.GetByIdsDynamic(ids2.OfType<decimal>().ToList(), returnFields);
                        }
                        else
                        {
                            data2 = mapTable.GetByIdsDynamic(ids2.ToDynamicList(), returnFields);
                        }

                    }
                    else
                    {
                        if (t2 == typeof(long))
                        {
                            data2 = new List<T3> { mapTable.GetByIdDynamic((long)first2, returnFields) };
                        }
                        else if (t2 == typeof(string))
                        {
                            data2 = new List<T3> { mapTable.GetByIdDynamic((string)first2, returnFields) };
                        }
                        else if (t2 == typeof(int))
                        {
                            data2 = new List<T3> { mapTable.GetByIdDynamic((int)first2, returnFields) };
                        }
                        else if (t2 == typeof(decimal))
                        {
                            data2 = new List<T3> { mapTable.GetByIdDynamic((decimal)first2, returnFields) };
                        }
                        else
                        {
                            data2 = new List<T3> { mapTable.GetByIdDynamic(first2, returnFields) };
                        }

                    }
                }
                else
                {
                    if (ids2Count == 1)
                    {
                        if (t2 == typeof(long))
                        {
                            data2 = mapTable.GetByIdsWithFieldDynamic(ids2.OfType<long>().ToList(), mapField, returnFields);
                        }
                        else if (t2 == typeof(string))
                        {
                            data2 = mapTable.GetByIdsWithFieldDynamic(ids2.OfType<string>().ToList(), mapField, returnFields);
                        }
                        else if (t2 == typeof(int))
                        {
                            data2 = mapTable.GetByIdsWithFieldDynamic(ids2.OfType<int>().ToList(), mapField, returnFields);
                        }
                        else if (t2 == typeof(decimal))
                        {
                            data2 = mapTable.GetByIdsWithFieldDynamic(ids2.OfType<decimal>().ToList(), mapField, returnFields);
                        }
                        else
                        {
                            data2 = mapTable.GetByIdsWithFieldDynamic(ids2.ToDynamicList(), mapField, returnFields);
                        }

                    }
                    else
                    {
                        if (t2 == typeof(long))
                        {
                            data2 = mapTable.GetByWhereDynamic($"WHERE {mapField}=@id", new { id = (long)first2 }, returnFields);
                        }
                        else if (t2 == typeof(string))
                        {
                            data2 = mapTable.GetByWhereDynamic($"WHERE {mapField}=@id", new { id = (string)first2 }, returnFields);
                        }
                        else if (t2 == typeof(int))
                        {
                            data2 = mapTable.GetByWhereDynamic($"WHERE {mapField}=@id", new { id = (int)first2 }, returnFields);
                        }
                        else if (t2 == typeof(decimal))
                        {
                            data2 = mapTable.GetByWhereDynamic($"WHERE {mapField}=@id", new { id = (decimal)first2 }, returnFields);
                        }
                        else
                        {
                            data2 = mapTable.GetByWhereDynamic($"WHERE {mapField}=@id", new { id = first2 }, returnFields);
                        }

                    }
                }
            }
            else
            {
                data2 = Enumerable.Empty<dynamic>();
            }

            list.MapManyToManyDynamic(field, propertyName, data, prevField, nextField, data2, mapField);

        }

        public static void MapTableManyToManyDynamic<T, T2, T3>(this IEnumerable<T> list, string field, string propertyName, ITable<T2> centerTable, string prevField, string nextField, ITable<T3> mapTable, string mapField, string returnFields, string and, DynamicParameters par = null, string orderby = null) where T : class where T2 : class where T3 : class
        {
            if (list == null || list.Count() == 0)
                return;
            var ids = list.AsQueryable().Where($"{field}!=null").Select(field).Distinct();
            var idsCount = ids.Count();
            if (idsCount == 0)
            {
                return;
            }
            var first = ids.First();
            Type t = first.GetType();
            IEnumerable<T2> data;
            if (idsCount > 1)
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

            var ids2 = data.AsQueryable().Where($"{nextField}!=null").Select(nextField).Distinct();
            var ids2Count = ids2.Count();
            IEnumerable<dynamic> data2;
            if (ids2Count > 0)
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
                data2 = mapTable.GetByWhereDynamic(where, par, returnFields, orderby);
            }
            else
            {
                data2 = Enumerable.Empty<T3>();
            }

            list.MapManyToMany(field, propertyName, data, prevField, nextField, data2, mapField);

        }


        #endregion

        #region ITable<T> map many to many

        public static IEnumerable<dynamic> MapCenterTableDynamic<T, T2>(this ITable<T> table, ITable<T2> centerTable, string prevField, string nextField, object id, string returnFields = null) where T : class where T2 : class
        {
            var where = $"AS A WHERE EXISTS(SELECT 1 FROM {centerTable.Name} WHERE {prevField}=A.{table.SqlField.PrimaryKey} AND {nextField}=@id)";
            return table.GetByWhereDynamic(where, new { id }, returnFields);
        }

        public static IEnumerable<dynamic> MapCenterTableDynamic<T, T2>(this ITable<T> table, ITable<T2> centerTable, string prevField, string nextField, object id, string returnFields, string and, DynamicParameters par = null, string orderby = null, int limit = 0) where T : class where T2 : class
        {
            if (par == null)
                par = new DynamicParameters();
            par.Add("@nextidddd", id);
            var where = $"AS A WHERE EXISTS(SELECT 1 FROM {centerTable.Name} WHERE {prevField}=A.{table.SqlField.PrimaryKey} AND {nextField}=@nextidddd) {and}";
            return table.GetByWhereDynamic(where, par, returnFields, orderby, limit);
        }

        public static PageEntity<dynamic> MapCenterTableDynamic<T, T2>(this ITable<T> table, ITable<T2> centerTable, string prevField, string nextField, object id, string returnFields, int page, int pageSize, string and = null, DynamicParameters par = null, string orderby = null) where T : class where T2 : class
        {
            if (par == null)
                par = new DynamicParameters();
            par.Add("@nextidddd", id);
            var where = $"AS A WHERE EXISTS(SELECT 1 FROM {centerTable.Name} WHERE {prevField}=A.{table.SqlField.PrimaryKey} AND {nextField}=@nextidddd) {and}";
            return table.GetByPageAndCountDynamic(page, pageSize, where, par, returnFields, orderby);
        }

        #endregion
    }
}
