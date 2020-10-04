using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace Dapper.Sharding
{
    internal class MySqlTableManager : ITableManager
    {
        public MySqlTableManager(string name, IDatabase database, IDbConnection conn = null, IDbTransaction tran = null, int? commandTimeout = null) : base(name, database, new DapperEntity(name, database, conn, tran, commandTimeout))
        {

        }

        public override ITableManager CreateTranManager(IDbConnection conn, IDbTransaction tran, int? commandTimeout = null)
        {
            return new MySqlTableManager(Name, DataBase, conn, tran, commandTimeout);
        }

        public override void CreateIndex(string name, string columns, IndexType indexType)
        {
            string sql = null;
            switch (indexType)
            {
                case IndexType.Normal: sql = $"CREATE INDEX `{name}` ON `{Name}` ({columns});"; break;
                case IndexType.Unique: sql = $"CREATE UNIQUE INDEX `{name}` ON `{Name}` ({columns});"; break;
                case IndexType.FullText: sql = $"CREATE FULLTEXT INDEX `{name}` ON `{Name}` ({columns});"; break;
                case IndexType.Spatial: sql = $"CREATE SPATIAL INDEX `{name}` ON `{Name}` ({columns});"; break;
            }
            DpEntity.Execute(sql);
        }

        public override void DropIndex(string name)
        {
            DpEntity.Execute($"ALTER TABLE {Name} DROP INDEX {name}");
        }

        public override void AlertIndex(string name, string columns, IndexType indexType)
        {
            DropIndex(name);
            CreateIndex(name, columns, indexType);
        }

        public override List<IndexEntity> GetIndexEntityList()
        {
            var list = new List<IndexEntity>();
            var indexList = DpEntity.Query($"SHOW INDEX FROM `{Name}`");
            var groupNames = indexList.Select(s => s.Key_name).Distinct();

            foreach (var g in groupNames)
            {
                var item = indexList.Where(f => f.Key_name == g).ToList();
                var entity = new IndexEntity();
                entity.Name = g;
                var item0 = item[0];

                if (item0.Key_name == "PRIMARY")
                {
                    entity.Type = IndexType.PrimaryKey;
                }
                else if (item0.Index_type == "BTREE" && item0.Non_unique == 0)
                {
                    entity.Type = IndexType.Unique;
                }
                else if (item0.Index_type == "FULLTEXT")
                {
                    entity.Type = IndexType.FullText;
                }
                else if (item0.Index_type == "SPATIAL")
                {
                    entity.Type = IndexType.Spatial;
                }
                else
                {
                    entity.Type = IndexType.Normal;
                }

                if (item.Count == 1)
                {
                    entity.Columns = item0.Column_name;
                }
                else
                {
                    foreach (var it in item)
                    {
                        entity.Columns += it.Column_name;
                        if (it != item.Last())
                        {
                            entity.Columns += ",";
                        }
                    }
                }

                list.Add(entity);
            }
            return list;
        }

        public override List<ColumnEntity> GetColumnEntityList(TableEntity tb = null)
        {
            var list = new List<ColumnEntity>();
            var columnList = DpEntity.Query($"SHOW FULL COLUMNS FROM `{Name}`");
            foreach (var item in columnList)
            {
                ColumnEntity model = new ColumnEntity();
                model.Name = ((string)item.Field).FirstCharToUpper(); //列名

                string columnType = item.Type;//数据类型

                if (string.IsNullOrEmpty(columnType))
                {
                    columnType = "(0)";
                }

                var array = columnType.Split('(');
                var t = array[0].ToLower();
                var map = DbCsharpTypeMap.MySqlMap.FirstOrDefault(f => f.DbType == t);
                if (map != null)
                    model.CsStringType = map.CsStringType;
                else
                    model.CsStringType = "object";

                model.CsType = map.CsType;
                model.DbType = t;

                if (array.Length == 2)
                {
                    var length = array[1].Split(')')[0];
                    model.DbLength = length;
                    model.Length = Convert.ToDouble(length.Replace(',', '.'));
                }
                else
                {
                    if (t.ToLower() == "text")
                    {
                        model.Length = -1;
                    }
                    else if (t.ToLower() == "longtext")
                    {
                        model.Length = -2;
                    }
                    else
                    {
                        model.Length = 0;
                        model.DbLength = "0";
                    }
                }
                model.Comment = item.Comment; //说明
                list.Add(model);
            }
            return list;
        }

        public override void ReName(string name)
        {
            DpEntity.Execute($"ALTER TABLE `{Name}` RENAME TO `{name}`");
        }

        public override void SetComment(string comment)
        {
            DpEntity.Execute($"ALTER TABLE `{Name}` COMMENT '{comment}'");
        }

        public override void SetCharset(string name)
        {
            DpEntity.Execute($"ALTER TABLE `{Name}` DEFAULT CHARACTER SET {name}");
        }

        public override void AddColumn(string name, Type t, double length = 0, string comment = null)
        {
            var dbType = CsharpTypeToDbType.Create(DataBase.Client.DbType, t, length);
            DpEntity.Execute($"ALTER TABLE `{Name}` ADD  `{name}` {dbType} COMMENT '{comment}'");
        }

        public override void DropColumn(string name)
        {
            DpEntity.Execute($"ALTER TABLE `{Name}` DROP COLUMN `{name}`");
        }

        public override void AddColumnAfter(string name, string afterName, Type t, double length = 0, string comment = null)
        {
            var dbType = CsharpTypeToDbType.Create(DataBase.Client.DbType, t, length);
            DpEntity.Execute($"ALTER TABLE `{Name}` ADD  `{name}` {dbType} COMMENT '{comment}' AFTER `{afterName}`");
        }

        public override void AddColumnFirst(string name, Type t, double length = 0, string comment = null)
        {
            var dbType = CsharpTypeToDbType.Create(DataBase.Client.DbType, t, length);
            DpEntity.Execute($"ALTER TABLE `{Name}` ADD  `{name}` {dbType} COMMENT '{comment}' FIRST");
        }

        public override void ModifyColumn(string name, Type t, double length = 0, string comment = null)
        {
            var dbType = CsharpTypeToDbType.Create(DataBase.Client.DbType, t, length);
            DpEntity.Execute($"ALTER TABLE `{Name}` MODIFY COLUMN `{name}` {dbType} COMMENT '{comment}'");
        }

        public override void ModifyColumnFirst(string name, Type t, double length = 0, string comment = null)
        {
            var dbType = CsharpTypeToDbType.Create(DataBase.Client.DbType, t, length);
            DpEntity.Execute($"ALTER TABLE `{Name}` MODIFY COLUMN `{name}` {dbType} COMMENT '{comment}' FIRST");
        }

        public override void ModifyColumnAfter(string name, string afterName, Type t, double length = 0, string comment = null)
        {
            var dbType = CsharpTypeToDbType.Create(DataBase.Client.DbType, t, length);
            DpEntity.Execute($"ALTER TABLE `{Name}` MODIFY COLUMN `{name}` {dbType} COMMENT '{comment}' AFTER `{afterName}`");
        }

        public override void ModifyColumnName(string oldName, string newName, Type t, double length = 0, string comment = null)
        {
            var dbType = CsharpTypeToDbType.Create(DataBase.Client.DbType, t, length);
            DpEntity.Execute($"ALTER TABLE `{Name}` CHANGE `{oldName}` `{newName}` {dbType} COMMENT '{comment}'");
        }
    }
}
