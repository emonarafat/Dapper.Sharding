using System;
using System.Collections.Generic;
using System.Linq;

namespace Dapper.Sharding
{
    internal class SqlServerTableManager : ITableManager
    {
        public SqlServerTableManager(string name, IDatabase database) : base(name, database)
        {

        }

        public override void CreateIndex(string name, string columns, IndexType indexType)
        {
            string sql = null;
            switch (indexType)
            {
                case IndexType.Normal: sql = $"CREATE INDEX {name} ON [{Name}] ({columns});"; break;
                case IndexType.Unique: sql = $"CREATE UNIQUE INDEX {name} ON [{Name}] ({columns});"; break;
            }
            DataBase.Execute(sql);
        }


        public override void DropIndex(string name)
        {
            DataBase.Execute($"DROP INDEX {name} ON [{Name}]");
        }

        public override void AlertIndex(string name, string columns, IndexType indexType)
        {
            DropIndex(name);
            CreateIndex(name, columns, indexType);
        }

        public override List<IndexEntity> GetIndexEntityList()
        {
            IEnumerable<dynamic> data = DataBase.Query($"EXEC sp_helpindex '{DataBase.Name}.dbo.{Name}'");
            var list = new List<IndexEntity>();
            foreach (var row in data)
            {
                var model = new IndexEntity();
                model.Name = row.index_name;
                model.Columns = row.index_keys;
                var descript = ((string)row.index_description).ToLower();
                if (descript.IndexOf("nonclustered") >= 0)
                {
                    if (descript.IndexOf("unique") >= 0)
                    {
                        model.Type = IndexType.Unique;
                    }
                    else
                    {
                        model.Type = IndexType.Normal;
                    }
                }
                else
                {
                    model.Type = IndexType.PrimaryKey;
                }

                list.Add(model);
            }
            return list;
        }

        public override List<ColumnEntity> GetColumnEntityList(TableEntity tb = null)
        {
            if (tb == null)
                tb = new TableEntity();
            string sql = @"SELECT  
ColumnName=a.name, 
IsKey=case when exists(SELECT 1 FROM sysobjects where xtype='PK' and name in (
  SELECT name FROM sysindexes WHERE indid in(
   SELECT indid FROM sysindexkeys WHERE id = a.id AND colid=a.colid 
   ))) then 1 else 0 end, 
IsIdentity=case when COLUMNPROPERTY(a.id,a.name,'IsIdentity')=1 then 1 else 0 end, 
ColumnType=b.name, 
ColumnLength=COLUMNPROPERTY(a.id,a.name,'PRECISION'), 
DecimalDigit =isnull(COLUMNPROPERTY(a.id,a.name,'Scale'),0), 
ColumnCommnent=isnull(g.[value],''),
AllowNull=case when a.isnullable=1 then 1 else 0 end, 
DefaultValue=isnull(e.text,'')
FROM syscolumns a 
left join systypes b on a.xtype=b.xusertype 
inner join sysobjects d on a.id=d.id and d.xtype='U' and d.name<>'dtproperties' 
left join syscomments e on a.cdefault=e.id 
left join sys.extended_properties g on a.id=g.major_id and a.colid=g.minor_id 
left join sys.extended_properties f on d.id=f.major_id and f.minor_id =0 
where d.name=@Name
order by a.id,a.colorder";

            var list = new List<ColumnEntity>();
            IEnumerable<dynamic> data = DataBase.Query(sql, new { Name });
            foreach (var row in data)
            {
                var model = new ColumnEntity();

                model.Name = ((string)row.ColumnName).FirstCharToUpper();
                model.Comment = row.ColumnCommnent;

                var t = (string)row.ColumnType;

                var map = DbCsharpTypeMap.SqlServerMap.FirstOrDefault(f => f.DbType == t);
                if (map != null)
                    model.CsStringType = map.CsStringType;
                else
                    model.CsStringType = "object";

                model.CsType = map.CsType;
                model.DbType = t.ToLower();

                if (model.DbType == "decimal")
                {
                    model.Length = Convert.ToDouble($"{row.ColumnLength}.{row.DecimalDigit}");
                    model.DbLength = $"{row.ColumnLength},{row.DecimalDigit}";
                }
                else
                {
                    model.Length = row.ColumnLength;
                    model.DbLength = row.ColumnLength.ToString();
                }

                if (row.IsKey == 1)
                {
                    tb.PrimaryKey = model.Name;
                    if (row.IsIdentity == 1)
                    {
                        tb.IsIdentity = true;
                    }
                    tb.PrimaryKeyType = model.CsType;
                }

                list.Add(model);
            }
            return list;
        }

        public override void AddColumn(string name, Type t, double length = 0, string comment = null)
        {
            var dbType = CsharpTypeToDbType.Create(DataBase.DbType, t, length);
            DataBase.Execute($"alter table [{Name}] add  [{name}] {dbType}");
            DataBase.Execute($"EXEC sp_addextendedproperty 'MS_Description', N'{comment}', 'SCHEMA', N'dbo','TABLE', N'{Name}','COLUMN', N'{name}'");
        }

        public override void AddColumnAfter(string name, string afterName, Type t, double length = 0, string comment = null)
        {
            throw new NotImplementedException();
        }

        public override void AddColumnFirst(string name, Type t, double length = 0, string comment = null)
        {
            throw new NotImplementedException();
        }

        public override void DropColumn(string name)
        {
            DataBase.Execute($"alter table [{Name}] drop column [{name}]");
        }


        public override void ModifyColumn(string name, Type t, double length = 0, string comment = null)
        {
            var dbType = CsharpTypeToDbType.Create(DataBase.DbType, t, length);
            DataBase.Execute($"alter table [{Name}] alter column [{name}] {dbType}");
        }

        public override void ModifyColumnAfter(string name, string afterName, Type t, double length = 0, string comment = null)
        {
            throw new NotImplementedException();
        }

        public override void ModifyColumnFirst(string name, Type t, double length = 0, string comment = null)
        {
            throw new NotImplementedException();
        }

        public override void ModifyColumnName(string oldName, string newName, Type t, double length = 0, string comment = null)
        {
            throw new NotImplementedException();
        }

        public override void ReName(string name)
        {
            throw new NotImplementedException();
        }

        public override void SetCharset(string name)
        {
            throw new NotImplementedException();
        }

        public override void SetComment(string comment)
        {
            throw new NotImplementedException();
        }
    }
}
