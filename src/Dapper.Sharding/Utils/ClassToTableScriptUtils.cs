using System.Linq;
using System.Text;

namespace Dapper.Sharding
{
    public class ClassToTableScriptUtils
    {
        public static string GetMySqlScript<T>(string name, string chartSet = "utf8")
        {
            var table = ClassToTableEntityUtils.Get<T>(name);
            var sb = new StringBuilder();
            sb.Append($"CREATE TABLE IF NOT EXISTS `{table.Name}` (");
            foreach (var item in table.ColumnList)
            {
                sb.Append($"`{item.Name}` {item.DbType}");
                if (!string.IsNullOrEmpty(table.PrimaryKey))
                {
                    if (table.PrimaryKey.ToLower() == item.Name.ToLower())
                    {
                        if (table.IsIdentity)
                        {
                            sb.Append(" AUTO_INCREMENT");
                        }
                        sb.Append(" PRIMARY KEY");
                    }
                }
                sb.Append($" COMMENT '{item.Comment}'");
                if (item != table.ColumnList.Last())
                {
                    sb.Append(",");
                }
            }

            if (table.IndexList != null && table.IndexList.Count > 0)
            {
                sb.Append(",");
                foreach (var ix in table.IndexList)
                {
                    if (ix.Type == IndexType.Normal)
                    {
                        sb.Append("KEY");
                    }
                    if (ix.Type == IndexType.Unique)
                    {
                        sb.Append("UNIQUE KEY");
                    }
                    if (ix.Type == IndexType.FullText)
                    {
                        sb.Append("FULLTEXT KEY");
                    }
                    if (ix.Type == IndexType.Spatial)
                    {
                        sb.Append("SPATIAL KEY");
                    }
                    sb.Append($" `{ix.Name}` ({ix.Columns})");
                    if (ix != table.IndexList.Last())
                    {
                        sb.Append(",");
                    }
                }
            }
            sb.Append($")DEFAULT CHARSET={chartSet} COMMENT '{table.Comment}'");

            return sb.ToString();
        }
    }
}
