using Npgsql;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

namespace Dapper.Sharding
{
    internal class PostgreDatabase : IDatabase
    {
        public PostgreDatabase(string name, PostgreClient client) : base(name, client)
        {
            ConnectionString = ConnectionStringBuilder.BuilderPostgresql(client.Config, name);
        }

        public override string ConnectionString { get; }

        protected override ITable<T> CreateITable<T>(string name)
        {
            return new PostgreTable<T>(name, this);
        }

        public override ITableManager GetTableManager(string name)
        {
            return new PostgreTableManager(name, this);
        }

        public override void DropTable(string name)
        {
            using (var conn = GetConn())
            {
                conn.Execute($"DROP TABLE IF EXISTS {name}");
            }
            TableCache.TryRemove(name.ToLower(), out _);
        }

        public override bool ExistsTable(string name)
        {
            using (var conn = GetConn())
            {
                return conn.ExecuteScalar<int>($"select count(1) from pg_class where relname='{name}'") > 0;
            }
        }

        public override IDbConnection GetConn()
        {
            var conn = new NpgsqlConnection(ConnectionString);
            if (conn.State != ConnectionState.Open)
                conn.Open();
            return conn;
        }

        public override async Task<IDbConnection> GetConnAsync()
        {
            var conn = new NpgsqlConnection(ConnectionString);
            if (conn.State != ConnectionState.Open)
                await conn.OpenAsync();
            return conn;
        }

        public override IEnumerable<string> GetTableColumnList(string name)
        {
            using (var conn = GetConn())
            {
                return conn.Query<string>($"select column_name from information_schema.columns where table_schema='public' and table_name = '{name}'");
            }
        }

        public override TableEntity GetTableEntityFromDatabase(string name)
        {
            var entity = new TableEntity();

            string sql = $@"select a.relname as name , b.description as value from pg_class a 
left join (select * from pg_description where objsubid =0 ) b on a.oid = b.objoid
where a.relname='{name}' and a.relname in (select tablename from pg_tables where schemaname = 'public')
order by a.relname asc";

            using (var conn = GetConn())
            {
                var row = conn.QueryFirstOrDefault(sql);
                if (row != null)
                {
                    entity.Comment = row.value;
                }
            }

            var manager = GetTableManager(name);
            var indexList = manager.GetIndexEntityList();
            entity.IndexList = indexList;
            var ix = indexList.FirstOrDefault(f => f.Type == IndexType.PrimaryKey);
            if (ix != null)
            {
                entity.PrimaryKey = ix.Columns.FirstCharToUpper();
            }
            entity.ColumnList = manager.GetColumnEntityList(entity);

            var col = entity.ColumnList.FirstOrDefault(w => w.Name.ToLower() == entity.PrimaryKey.ToLower());
            if (col != null)
            {
                entity.PrimaryKeyType = col.CsType;
            }
            return entity;

        }

        public override IEnumerable<string> GetTableList()
        {
            using (var conn = GetConn())
            {
                return conn.Query<string>("select tablename from pg_tables where schemaname='public'");
            }
        }

        public override string GetTableScript<T>(string name)
        {
            throw new NotImplementedException();
        }

        public override void SetCharset(string chartset)
        {
            throw new NotImplementedException();
        }

        public override void TruncateTable(string name)
        {
            using (var conn = GetConn())
            {
                conn.Execute($"TRUNCATE TABLE {name}");
            }
        }


    }
}
