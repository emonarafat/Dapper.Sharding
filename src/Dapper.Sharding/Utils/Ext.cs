using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Dapper.Sharding
{
    internal static class Ext
    {
        public static string FirstCharToUpper(this string txt)
        {
            return txt.Substring(0, 1).ToUpper() + txt.Substring(1);
        }

        public static string FirstCharToLower(this string txt)
        {
            return txt.Substring(0, 1).ToLower() + txt.Substring(1);
        }

        public static void Using<T>(this ITable<T> table, Action action)
        {
            if (table.Conn == null)
            {
                using (table.Conn = table.DataBase.GetConn())
                {
                    action();
                    table.Conn = null;
                }
            }
            else
            {
                action();
            }
        }

        public static T Using<T, T2>(this ITable<T2> table, Func<T> action)
        {
            if (table.Conn == null)
            {
                using (table.Conn = table.DataBase.GetConn())
                {
                    var result = action();
                    table.Conn = null;
                    return result;
                }
            }
            else
            {
                return action();
            }
        }

        public static void Using(this ITableManager table, Action action)
        {
            if (table.Conn == null)
            {
                using (table.Conn = table.DataBase.GetConn())
                {
                    action();
                    table.Conn = null;
                }
            }
            else
            {
                action();
            }
        }

        public static T Using<T>(this ITableManager table, Func<T> action)
        {
            if (table.Conn == null)
            {
                using (table.Conn = table.DataBase.GetConn())
                {
                    var result = action();
                    table.Conn = null;
                    return result;
                }
            }
            else
            {
                return action();
            }
        }

        public static void CreateFiles(this IDatabase database, string savePath, string tableList = "*", string nameSpace = "Model", string Suffix = "Table")
        {
            List<TableEntity> tableEntityList;
            if (tableList == "*")
            {
                tableEntityList = database.GetTableEnitysFromDatabase();
            }
            else if (tableList.Contains(","))
            {
                tableEntityList = new List<TableEntity>();
                var array = tableList.Split(',');
                foreach (var item in array)
                {
                    tableEntityList.Add(database.GetTableEntityFromDatabase(item));
                }
            }
            else
            {
                tableEntityList = new List<TableEntity>();
                tableEntityList.Add(database.GetTableEntityFromDatabase(tableList));
            }

            foreach (var table in tableEntityList)
            {
                var sb = new StringBuilder();
                sb.Append("using System;");
                sb.Append("\r\n");
                sb.Append("using Dapper.Sharding;");
                sb.Append("\r\n");
                sb.Append($"namespace {nameSpace}");
                sb.Append("\r\n");
                sb.Append("{");
                sb.Append("\r\n");
                sb.Append($"    [Table(\"{table.PrimaryKey}\", {table.IsIdentity.ToString().ToLower()}, \"{table.Comment}\")]");

                sb.Append("\r\n");
                sb.Append("}");
                var path = Path.Combine(savePath, "test.cs");
                File.WriteAllText(savePath, sb.ToString(), Encoding.UTF8);
            }

        }

    }
}
