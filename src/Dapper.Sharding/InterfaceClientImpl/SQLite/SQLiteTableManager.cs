using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace Dapper.Sharding
{
    internal class SQLiteTableManager : ITableManager
    {
        public SQLiteTableManager(string name, IDatabase database, IDbConnection conn = null, IDbTransaction tran = null, int? commandTimeout = null) : base(name, database, new DapperEntity(name, database, conn, tran, commandTimeout))
        {

        }

        public override ITableManager CreateTranManager(IDbConnection conn, IDbTransaction tran, int? commandTimeout = null)
        {
            return new SQLiteTableManager(Name, DataBase, conn, tran, commandTimeout);
        }

        public override void CreateIndex(string name, string columns, IndexType indexType)
        {
            string sql = null;
            switch (indexType)
            {
                case IndexType.Normal: sql = $"CREATE INDEX {name} ON [{Name}] ({columns});"; break;
                case IndexType.Unique: sql = $"CREATE UNIQUE INDEX {name} ON [{Name}] ({columns});"; break;
            }
            DpEntity.Execute(sql);
        }


        public override void DropIndex(string name)
        {
            DpEntity.Execute($"DROP INDEX {name}");
        }

        public override void AlertIndex(string name, string columns, IndexType indexType)
        {
            DropIndex(name);
            CreateIndex(name, columns, indexType);
        }
        public override List<IndexEntity> GetIndexEntityList()
        {
            IEnumerable<dynamic> data = DpEntity.Query($"SELECT * FROM sqlite_master where type='index' AND tbl_name='{Name}' AND Name NOT LIKE 'sqlite_autoindex%';");
            var list = new List<IndexEntity>();
            foreach (var row in data)
            {
                var model = new IndexEntity();
                model.Name = row.name;
                var indexsql = (string)row.sql;
                model.Columns = indexsql.Split('(', ')')[1];
                if (indexsql.ToUpper().IndexOf("UNIQUE") >= 0)
                {
                    model.Type = IndexType.Unique;
                }
                else
                {
                    model.Type = IndexType.Normal;
                }
                list.Add(model);
            }

            return list;
        }

        public override List<ColumnEntity> GetColumnEntityList()
        {
            var list = new List<ColumnEntity>();
            IEnumerable<dynamic> data = DpEntity.Query($"pragma table_info('{Name}')");
            foreach (var row in data)
            {
                var model = new ColumnEntity();
                model.Name = ((string)row.name).FirstCharToUpper(); //列名
                string columnType = row.type;//数据类型
                var map = DbCsharpTypeMap.SqLiteMap.FirstOrDefault(f => f.DbType == columnType);
                if (map != null)
                    model.CsStringType = map.CsStringType;
                else
                    model.CsStringType = "object";
                model.CsType = map.CsType;
                model.DbType = columnType;
                list.Add(model);

            }
            return list;
        }



        public override void AddColumn(string name, Type t, double length = 0, string comment = null)
        {
            var dbType = CsharpTypeToDbType.Create(DataBase.Client.DbType, t, length);
            DpEntity.Execute($"ALTER TABLE {Name} ADD COLUMN {name} {dbType}");
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
            throw new NotImplementedException();
        }

        public override void ModifyColumn(string name, Type t, double length = 0, string comment = null)
        {
            throw new NotImplementedException();
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
            DpEntity.Execute($"ALTER TABLE {Name} RENAME TO {name}");
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
