using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
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

        public static void CreateFiles(this IDatabase database, string savePath, string tableList = "*", string nameSpace = "Model", string Suffix = "Table", bool partialClass = false)
        {
            IEnumerable<string> list;
            if (tableList == "*")
            {
                list = database.ShowTableList();
            }
            else if (tableList.Contains(","))
            {
                list = tableList.Split(',').ToList(); ;
            }
            else
            {
                list = new List<string> { tableList };
            }

            foreach (var name in list)
            {
                var entity = database.GetTableEntityFromDatabase(name);
                var className = name.FirstCharToUpper() + Suffix;
                var sb = new StringBuilder();
                sb.Append("using System;");
                sb.AppendLine();
                sb.Append("using Dapper.Sharding;");
                sb.AppendLine();
                sb.AppendLine();
                sb.Append($"namespace {nameSpace}");
                sb.AppendLine();
                sb.Append("{");
                sb.AppendLine();
                var indexList = entity.IndexList.Where(w => w.Type != IndexType.PrimaryKey);
                foreach (var item in indexList)
                {
                    sb.Append($"    [Index(\"{item.Name}\", \"{item.Columns}\", {item.StringType})]");

                    sb.AppendLine();
                }
                sb.Append($"    [Table(\"{entity.PrimaryKey}\", {entity.IsIdentity.ToString().ToLower()}, \"{entity.Comment}\")]");
                sb.AppendLine();
                if (partialClass)
                {
                    sb.Append($"    public partial class {className}");
                }
                else
                {
                    sb.Append($"    public class {className}");
                }
                sb.AppendLine();
                sb.Append("    {");
                sb.AppendLine();
                foreach (var item in entity.ColumnList)
                {
                    sb.Append($"        [Column(\"{item.Comment}\", {item.Length})]");
                    sb.AppendLine();
                    sb.Append("        public " + item.CsStringType + " " + item.Name + " { get; set; }");
                    sb.AppendLine();
                    if (item != entity.ColumnList.Last())
                    {
                        sb.AppendLine();
                    }
                }
                sb.AppendLine();
                sb.Append("    }");
                sb.AppendLine();
                sb.AppendLine();
                sb.Append("}");

                var path = Path.Combine(savePath, $"{className}.cs");
                File.WriteAllText(path, sb.ToString(), Encoding.UTF8);
            }

        }

    }
}
