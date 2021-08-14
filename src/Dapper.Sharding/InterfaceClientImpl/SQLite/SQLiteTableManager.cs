using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Dapper.Sharding
{
    internal class SQLiteTableManager : ITableManager
    {
        public SQLiteTableManager(string name, IDatabase database) : base(name, database)
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

        public override async Task CreateIndexAsync(string name, string columns, IndexType indexType)
        {
            string sql = null;
            switch (indexType)
            {
                case IndexType.Normal: sql = $"CREATE INDEX {name} ON [{Name}] ({columns});"; break;
                case IndexType.Unique: sql = $"CREATE UNIQUE INDEX {name} ON [{Name}] ({columns});"; break;
            }
            await DataBase.ExecuteAsync(sql);
        }


        public override void DropIndex(string name)
        {
            DataBase.Execute($"DROP INDEX {name}");
        }


        public override async Task DropIndexAsync(string name)
        {
            await DataBase.ExecuteAsync($"DROP INDEX {name}");
        }


        public override void AddColumn(string name, Type t, double length = 0, string comment = null)
        {
            var dbType = CsharpTypeToDbType.Create(DataBase.DbType, t, length);
            if (t.IsValueType && t != typeof(DateTime) && t != typeof(DateTimeOffset))
            {
                dbType += " DEFAULT 0";   
            }
            DataBase.Execute($"ALTER TABLE {Name} ADD COLUMN {name} {dbType}");
        }

        public override async Task AddColumnAsync(string name, Type t, double length = 0, string comment = null)
        {
            var dbType = CsharpTypeToDbType.Create(DataBase.DbType, t, length);
            if (t.IsValueType && t != typeof(DateTime) && t != typeof(DateTimeOffset))
            {
                dbType += " DEFAULT 0";
            }
            await DataBase.ExecuteAsync($"ALTER TABLE {Name} ADD COLUMN {name} {dbType}");
        }


        public override void DropColumn(string name)
        {
            //throw new NotImplementedException();
        }

        public override Task DropColumnAsync(string name)
        {
            //throw new NotImplementedException();
            return default;
        }

        public override void ModifyColumn(string name, Type t, double length = 0, string comment = null)
        {
            throw new NotImplementedException();
        }

        public override Task ModifyColumnAsync(string name, Type t, double length = 0, string comment = null)
        {
            throw new NotImplementedException();
        }

        public override List<IndexEntity> GetIndexEntityList()
        {
            IEnumerable<dynamic> data = DataBase.Query($"SELECT * FROM sqlite_master where type='index' AND tbl_name='{Name}' AND Name NOT LIKE 'sqlite_autoindex%';");
            var list = new List<IndexEntity>();
            foreach (var row in data)
            {
                var model = new IndexEntity();
                model.Name = row.name;
                var indexsql = (string)row.sql;
                model.Columns = indexsql.Split('(', ')')[1];
                if (indexsql.ToUpper().Contains("UNIQUE"))
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

        public override async Task<List<IndexEntity>> GetIndexEntityListAsync()
        {
            IEnumerable<dynamic> data = await DataBase.QueryAsync($"SELECT * FROM sqlite_master where type='index' AND tbl_name='{Name}' AND Name NOT LIKE 'sqlite_autoindex%';");
            var list = new List<IndexEntity>();
            foreach (var row in data)
            {
                var model = new IndexEntity();
                model.Name = row.name;
                var indexsql = (string)row.sql;
                model.Columns = indexsql.Split('(', ')')[1];
                if (indexsql.ToUpper().Contains("UNIQUE"))
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

        public override List<ColumnEntity> GetColumnEntityList(TableEntity tb = null)
        {
            if (tb == null)
                tb = new TableEntity();
            var list = new List<ColumnEntity>();
            IEnumerable<dynamic> data = DataBase.Query($"pragma table_info('{Name}')");
            foreach (var row in data)
            {
                var model = new ColumnEntity();
                model.Name = ((string)row.name).FirstCharToUpper(); //列名
                string columnType = row.type;//数据类型
                model.DbType = columnType;
                var map = DbCsharpTypeMap.SqLiteMap.FirstOrDefault(f => f.DbType == columnType);
                if (map != null)
                {
                    model.CsStringType = map.CsStringType;
                    model.CsType = map.CsType;
                }
                else
                {
                    model.CsStringType = "object";
                    model.CsType = typeof(object);
                }
                list.Add(model);

            }
            return list;
        }

        public override async Task<List<ColumnEntity>> GetColumnEntityListAsync(TableEntity tb = null)
        {
            if (tb == null)
                tb = new TableEntity();
            var list = new List<ColumnEntity>();
            IEnumerable<dynamic> data = await DataBase.QueryAsync($"pragma table_info('{Name}')");
            foreach (var row in data)
            {
                var model = new ColumnEntity();
                model.Name = ((string)row.name).FirstCharToUpper(); //列名
                string columnType = row.type;//数据类型
                model.DbType = columnType;
                var map = DbCsharpTypeMap.SqLiteMap.FirstOrDefault(f => f.DbType == columnType);
                if (map != null)
                {
                    model.CsStringType = map.CsStringType;
                    model.CsType = map.CsType;
                }
                else
                {
                    model.CsStringType = "object";
                    model.CsType = typeof(object);
                }
                list.Add(model);

            }
            return list;
        }
    }
}
