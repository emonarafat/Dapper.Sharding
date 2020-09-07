using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;

namespace Dapper.Sharding
{
    internal class ClassToTableEntityUtils
    {
        public static TableEntity Get<T>()
        {
            var entity = new TableEntity();
            entity.ColumnList = new List<ColumnEntity>();
            entity.IndexList = new List<IndexEntity>();
            var t = typeof(T);
            var tableAttr = t.GetCustomAttributes(false).First(f => f is TableAttribute) as TableAttribute;
            entity.PrimaryKey = tableAttr.PrimaryKey;
            entity.IsIdentity = tableAttr.IsIdentity;
            entity.Comment = tableAttr.Comment;
            var indexAttrs = t.GetCustomAttributes(false).Where(f => f is IndexAttribute).Select(s => s as IndexAttribute);
            if (indexAttrs.Any())
            {
                foreach (var ix in indexAttrs)
                {
                    entity.IndexList.Add(new IndexEntity() { Name = ix.Name, Columns = ix.Columns, Type = ix.Indextype });
                }
            }

            var proList = t.GetProperties();
            foreach (var pro in proList)
            {
                var ignoreAttr = pro.GetCustomAttributes(false).FirstOrDefault(f => f is IgnoreAttribute);
                if (ignoreAttr != null)
                    continue;
                var column = new ColumnEntity();
                column.Name = pro.Name;
                column.CsType = pro.PropertyType;
                column.DbType = CsharpTypeToDbType.CreateMySqlType(column.CsType);
                if (column.Name.ToLower() == entity.PrimaryKey.ToLower())
                {
                    entity.PrimaryKeyType = column.CsType;
                }
                var colAttr = pro.GetCustomAttributes(false).FirstOrDefault(f => f is ColumnAttribute) as ColumnAttribute;
                if (colAttr != null)
                {
                    column.Comment = colAttr.Comment;
                    column.DbType = CsharpTypeToDbType.CreateMySqlType(column.CsType, colAttr.Length);
                }
                entity.ColumnList.Add(column);
            }

            return entity;
        }
    
    }
}
