using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;

namespace Dapper.Sharding
{
    public interface IClient
    {
        LockManager Locker { get; }

        ConcurrentDictionary<string, IDatabase> DataBaseCache { get; }

        string ConnectionString { get; }

        IDbConnection GetConn();

        Task<IDbConnection> GetConnAsync();

        void CreateDatabase(string name);

        void DropDatabase(string name);

        IEnumerable<string> ShowDatabases();

        IEnumerable<string> ShowDatabasesExcludeSystem();

        bool ExistsDatabase(string name);

        IDatabase GetDatabase(string name);

        string Charset { get; set; }

        bool AutoCreateDatabase { get; set; }

        bool AutoCreateTable { get; set; }

        bool AutoCompareTableColumn { get; set; }

        DataBaseType DbType { get; }

        void ClearCache();
    }
}
