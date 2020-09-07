using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dapper.Sharding
{
    internal class PostgreClient : IClient
    {
        public PostgreClient(string connectionString)
        {
            ConnectionString = connectionString;
        }

        public LockManager Locker => throw new NotImplementedException();

        public ConcurrentDictionary<string, IDatabase> DataBaseCache => throw new NotImplementedException();

        public string ConnectionString { get; }

        public string Charset { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
        public bool AutoCreateDatabase { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
        public bool AutoCreateTable { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
        public bool AutoCompareTableColumn { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

        public DataBaseType DbType => throw new NotImplementedException();

        public void ClearCache()
        {
            throw new NotImplementedException();
        }

        public void CreateDatabase(string name)
        {
            throw new NotImplementedException();
        }

        public void DropDatabase(string name)
        {
            throw new NotImplementedException();
        }

        public bool ExistsDatabase(string name)
        {
            throw new NotImplementedException();
        }

        public IDbConnection GetConn()
        {
            throw new NotImplementedException();
        }

        public IDatabase GetDatabase(string name)
        {
            throw new NotImplementedException();
        }

        public IEnumerable<string> ShowDatabases()
        {
            throw new NotImplementedException();
        }

        public IEnumerable<string> ShowDatabasesExcludeSystem()
        {
            throw new NotImplementedException();
        }
    }
}
