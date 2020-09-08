using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dapper.Sharding
{
    internal class SqlServerDatabase : IDatabase
    {
        public SqlServerDatabase(string name, SqlServerClient client)
        {
            Name = name;
            Client = client;
        }

        public string Name { get; }

        public LockManager Locker { get; } = new LockManager();

        public HashSet<string> TableCache { get; } = new HashSet<string>();

        public IClient Client { get; }

        public void CreateTable<T>(string name)
        {
            throw new NotImplementedException();
        }

        public void DropTable(string name)
        {
            using (var conn = GetConn())
            {
                conn.Execute($"IF EXISTS(SELECT 1 FROM sysObjects WHERE Id=OBJECT_ID(N'{name}') AND xtype='U')DROP TABLE [{name}]");
            }
            TableCache.Remove(name.ToLower());
        }

        public bool ExistsTable(string name)
        {
            using (var conn = GetConn())
            {
                return conn.ExecuteScalar($"SELECT 1 FROM sysObjects WHERE Id=OBJECT_ID(N'{name}') AND xtype='U'") != null;
            }
        }

        public void GeneratorClassFile(string savePath, string tableName = "*", string nameSpace = "Model", string Suffix = "Table", bool partialClass = false)
        {
            throw new NotImplementedException();
        }

        public IDbConnection GetConn()
        {
            var conn = Client.GetConn();
            if (conn.Database != Name)
                conn.ChangeDatabase(Name);
            return conn;
        }

        public ITable<T> GetTable<T>(string name)
        {
            throw new NotImplementedException();
        }

        public List<TableEntity> GetTableEnityListFromDatabase()
        {
            throw new NotImplementedException();
        }

        public TableEntity GetTableEntityFromDatabase(string name)
        {
            throw new NotImplementedException();
        }

        public ITableManager GetTableManager(string name, IDbConnection conn = null, IDbTransaction tran = null, int? commandTimeout = null)
        {
            throw new NotImplementedException();
        }

        public void SetCharset(string chartset)
        {
            throw new NotImplementedException();
        }

        public IEnumerable<string> ShowTableList()
        {
            using (var conn = GetConn())
            {
                return conn.Query<string>($"SELECT name FROM sysObjects WHERE xtype='U'");
            }
        }

        public string ShowTableScript<T>(string name)
        {
            throw new NotImplementedException();
        }

        public dynamic ShowTableStatus(string name)
        {
            throw new NotImplementedException();
        }

        public IEnumerable<dynamic> ShowTableStatusList()
        {
            throw new NotImplementedException();
        }

        public void TruncateTable(string name)
        {
            using (var conn = GetConn())
            {
                conn.Execute($"TRUNCATE TABLE [{name}]");
            }
        }

        public void Using(Action<IDbConnection> action)
        {
            using (var conn = GetConn())
            {
                action(conn);
            }
        }

        public void UsingTran(Action<IDbConnection, IDbTransaction> action)
        {
            using (var conn = GetConn())
            {
                using (var tran = conn.BeginTransaction())
                {
                    action(conn, tran);
                }
            }
        }
    }
}
