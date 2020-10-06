using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dapper.Sharding
{
    internal class OracleDatabase : IDatabase
    {
        public OracleDatabase(string name, OracleClient client) : base(name, client)
        {
            ConnectionString = ConnectionStringBuilder.BuilderOracle(client.Config, name);
        }

        public override string ConnectionString { get; }

        public override ITableManager GetTableManager(string name)
        {
            return new OracleTableManager(name, this);
        }

        protected override ITable<T> CreateITable<T>(string name)
        {
            return new OracleTable<T>(name, this);
        }

        public override void DropTable(string name)
        {
            using (var conn = GetConn())
            {
                conn.Execute("DROP TABLE " + name);
            }
            TableCache.TryRemove(name.ToLower(), out _);
        }

        public override bool ExistsTable(string name)
        {
            using (var conn = GetConn())
            {
                return conn.ExecuteScalar<int>($"SELECT COUNT(*) FROM ALL_TABLES WHERE OWNER='{Client.Config.UserId.ToUpper()}' AND TABLE_NAME='{name.ToUpper()}'") > 0;
            }
        }

        public override IDbConnection GetConn()
        {
            var conn = new OracleConnection(ConnectionString);
            if (conn.State != ConnectionState.Open)
                conn.Open();
            return conn;
        }

        public override async Task<IDbConnection> GetConnAsync()
        {
            var conn = new OracleConnection(ConnectionString);
            if (conn.State != ConnectionState.Open)
                await conn.OpenAsync();
            return conn;
        }

        public override IEnumerable<string> GetTableColumnList(string name)
        {
            throw new NotImplementedException();
        }

        public override TableEntity GetTableEntityFromDatabase(string name)
        {
            throw new NotImplementedException();
        }

        public override IEnumerable<string> GetTableList()
        {
            using (var conn = GetConn())
            {
                return conn.Query<string>($"SELECT OBJECT_NAME FROM DBA_OBJECTS WHERE OWNER='{Client.Config.UserId.ToUpper()}' AND OBJECT_TYPE='TABLE'");
            }
        }

        public override string GetTableScript<T>(string name)
        {           
            var tableEntity = ClassToTableEntityUtils.Get<T>(Client.DbType);
            if (tableEntity.IsIdentity)
                throw new Exception($"oracle is not supported identity key,table name is {name}");
            var sb = new StringBuilder();
            sb.Append($"CREATE TABLE {name.ToUpper()} (");
            foreach (var item in tableEntity.ColumnList)
            {
                sb.Append($"{item.Name.ToUpper()} {item.DbType}");
                if (tableEntity.PrimaryKey.ToLower() == item.Name.ToLower())
                {
                    sb.Append(" PRIMARY KEY");
                }
                if (item != tableEntity.ColumnList.Last())
                {
                    sb.Append(",");
                }
            }
            sb.Append(")");
            return sb.ToString();
        }

        public override void SetCharset(string chartset)
        {
            throw new NotImplementedException();
        }

        public override void TruncateTable(string name)
        {
            using (var conn = GetConn())
            {
                conn.Execute("TRUNCATE TABLE " + name);
            }
        }

        public override void CreateTable<T>(string name)
        {
            var script = GetTableScript<T>(name);
            var tableEntity = ClassToTableEntityUtils.Get<T>(Client.DbType);
            using (var conn = GetConn())
            {
                using (var tran = conn.BeginTransaction())
                {
                    try
                    {
                        conn.Execute(script);

                        foreach (var item in tableEntity.IndexList)
                        {
                            if (item.Type == IndexType.Normal)
                            {
                                conn.Execute($"CREATE INDEX {Client.Config.UserId.ToUpper()}.\"{name}_{item.Name}\" ON {Client.Config.UserId.ToUpper()}.\"{name.ToUpper()}\" ({item.Columns})");
                            }
                            else if (item.Type == IndexType.Unique)
                            {
                                conn.Execute($"CREATE UNIQUE INDEX {Client.Config.UserId.ToUpper()}.\"{name}_{item.Name}\" ON {Client.Config.UserId.ToUpper()}.\"{name.ToUpper()}\" ({item.Columns})");
                            }
                        }
                        tran.Commit();
                    }
                    catch (Exception ex)
                    {
                        tran.Rollback();
                        throw ex;
                    }

                }

            }
        }
    }
}
