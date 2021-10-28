using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;

namespace Dapper.Sharding
{
    internal static class Ext
    {
        #region string
        public static string FirstCharToUpper(this string txt)
        {
            return txt.Substring(0, 1).ToUpper() + txt.Substring(1);
        }

        public static string FirstCharToLower(this string txt)
        {
            return txt.Substring(0, 1).ToLower() + txt.Substring(1);
        }

        public static string SetOrderBy(this string str, string key)
        {
            if (string.IsNullOrEmpty(str))
            {
                return "ORDER BY " + key;
            }
            return "ORDER BY " + str;
        }

        #endregion

        #region IDatabase

        public static void CreateTableFiles(this IDatabase database, string savePath, List<string> tableList = null, string nameSpace = "Model", string suffix = "", bool partialClass = false, bool fisrtCharUpper = false)
        {
            if (!Directory.Exists(savePath))
            {
                Directory.CreateDirectory(savePath);
            }
            if (tableList == null || tableList.Count == 0)
            {
                tableList = database.GetTableList().ToList();
            }
            foreach (var name in tableList)
            {
                var entity = database.GetTableEntityFromDatabase(name);
                if (database.DbType == DataBaseType.ClickHouse)
                {
                    entity.IsIdentity = false;
                    entity.PrimaryKey = entity.ColumnList[0].Name;
                }
                var className = name.FirstCharToUpper() + suffix;
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
                if (entity.IndexList != null)
                {
                    var indexList = entity.IndexList.Where(w => w.Type != IndexType.PrimaryKey);
                    foreach (var item in indexList)
                    {
                        sb.Append($"    [Index(\"{item.Name}\", \"{item.Columns}\", {item.StringType})]");
                        sb.AppendLine();
                    }
                }
                if (!string.IsNullOrEmpty(entity.Comment))
                {
                    entity.Comment = entity.Comment.Replace("\r", "").Replace("\n", "");
                }
                sb.Append($"    [Table(\"{entity.PrimaryKey}\", {entity.IsIdentity.ToString().ToLower()}, \"{entity.Comment}\")]");
                sb.AppendLine();
                if (partialClass)
                {
                    sb.Append($"    public partial class {className}");
                    sb.AppendLine();
                }
                else
                {
                    sb.Append($"    public class {className}");
                    sb.AppendLine();
                }
                sb.Append("    {");
                sb.AppendLine();
                foreach (var item in entity.ColumnList)
                {
                    if (item.Length != 0 || !string.IsNullOrEmpty(item.Comment))
                    {
                        if (!string.IsNullOrEmpty(item.Comment))
                        {
                            item.Comment = item.Comment.Replace("\r", "").Replace("\n", "");
                        }
                        sb.Append($"        [Column({item.Length}, \"{item.Comment}\")]");
                        sb.AppendLine();
                    }
                    if (database.DbType == DataBaseType.ClickHouse)
                    {
                        sb.Append("        public " + item.CsStringType + " " + item.Name + " { get; set; }");
                        sb.AppendLine();
                    }
                    else
                    {
                        if (fisrtCharUpper)
                        {
                            sb.Append("        public " + item.CsStringType + " " + item.Name.FirstCharToUpper() + " { get; set; }");
                        }
                        else
                        {
                            sb.Append("        public " + item.CsStringType + " " + item.Name + " { get; set; }");
                        }
                        sb.AppendLine();
                    }
                    if (item != entity.ColumnList.Last())
                    {
                        sb.AppendLine();
                    }
                }
                sb.Append("    }");
                sb.AppendLine();
                sb.Append("}");

                var path = Path.Combine(savePath, $"{className}.cs");
                File.WriteAllText(path, sb.ToString(), Encoding.UTF8);
            }

        }

        public static void CreateDbContextFile(this IDatabase database, string savePath, string nameSpace, string modelNameSpace = "Model", string modelSuffix = "", bool proSuffix = false, bool tableNameLower = false, bool staticClass = true)
        {
            if (!Directory.Exists(savePath))
            {
                Directory.CreateDirectory(savePath);
            }
            var tableList = database.GetTableList().ToList();
            var sb = new StringBuilder();
            sb.Append("using Dapper.Sharding;");
            sb.AppendLine();
            if (nameSpace != modelNameSpace)
            {
                sb.Append($"using {modelNameSpace};");
                sb.AppendLine();
                sb.AppendLine();
            }
            else
            {
                sb.AppendLine();
            }
            sb.Append($"namespace {nameSpace}");
            sb.AppendLine();
            sb.Append("{");
            sb.AppendLine();
            sb.Append("    public class DbContext");
            sb.AppendLine();
            sb.Append("    {");
            sb.AppendLine();
            if (!staticClass)
            {
                sb.Append("        public readonly IDatabase Db;");
                sb.AppendLine();
                sb.Append("        public DbContext(IDatabase db)");
                sb.AppendLine();
                sb.Append("        {");
                sb.AppendLine();
                sb.Append("            Db = db;");
                sb.AppendLine();
                sb.Append("        }");
            }
            else
            {
                sb.Append("        public static readonly IClient Client;");
                sb.AppendLine();
                sb.Append($"        public static IDatabase Db => Client.GetDatabase(\"{database.Name}\");");
                sb.AppendLine();
                sb.Append("        static DbContext()");
                sb.AppendLine();
                sb.Append("        {");
                sb.AppendLine();
                sb.Append("            Client = ShardingFactory.CreateClient(DataBaseType.MySql, new DataBaseConfig { Server = \"127.0.0.1\", UserId = \"root\", Password = \"123\" });");
                sb.AppendLine();
                sb.Append("            //Client = ShardingFactory.CreateClient(DataBaseType.Postgresql, new DataBaseConfig { Server = \"127.0.0.1\", UserId = \"postgres\", Password = \"123\" });");
                sb.AppendLine();
                sb.Append("            //Client = ShardingFactory.CreateClient(DataBaseType.Sqlite, new DataBaseConfig { Server = \"db\" });");
                sb.AppendLine();
                sb.Append("            //Client = ShardingFactory.CreateClient(DataBaseType.SqlServer2008, new DataBaseConfig { Server = \".\", UserId = \"sa\", Password = \"123\" });");
                sb.AppendLine();
                sb.Append("            //Client = ShardingFactory.CreateClient(DataBaseType.ClickHouse, new DataBaseConfig { Server = \"127.0.0.1\", UserId = \"default\", Password = \"\" });");
                sb.AppendLine();
                sb.Append("            //Client = ShardingFactory.CreateClient(DataBaseType.Oracle, new DataBaseConfig { Server = \"127.0.0.1\", UserId = \"\", Password = \"\" });");
                sb.AppendLine();
                sb.Append("        }");
            }
            sb.AppendLine();
            sb.AppendLine();
            var st = " static";
            if (!staticClass)
            {
                st = "";
            }
            foreach (var name in tableList)
            {
                var className = name.FirstCharToUpper() + modelSuffix;
                string className2;
                if (proSuffix)
                {
                    className2 = className;
                }
                else
                {
                    className2 = name.FirstCharToUpper();
                }
                var tablename = name;
                if (tableNameLower)
                {
                    tablename = tablename.ToLower();
                }
                sb.Append($"        public{st} ITable<{className}> {className2} => Db.GetTable<{className}>(\"{tablename}\");");
                sb.AppendLine();
            }
            sb.Append("    }");
            sb.AppendLine();
            sb.Append("}");
            var path = Path.Combine(savePath, "DbContext.cs");
            File.WriteAllText(path, sb.ToString(), Encoding.UTF8);
        }

        #endregion

        #region IEnumerable

        public static IEnumerable<T> ConcatItem<T>(this IEnumerable<T>[] arrayList)
        {
            var list = Enumerable.Empty<T>();
            foreach (var item in arrayList)
            {
                list = list.Concat(item);
            }
            return list;
        }

        #endregion

    }
}
