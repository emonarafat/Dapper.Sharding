using System.Collections.Generic;
using System.Data;

namespace Dapper.Sharding
{
    public interface IClient
    {
        LockManager Locker { get; }

        HashSet<string> DataBaseCache { get; }

        HashSet<string> TableCache { get; }

        string ConnectionString { get; }

        IDbConnection GetConn();

        void CreateDatabase(string name);

        void DropDatabase(string name);

        IEnumerable<string> GetAllDatabase();

        IDatabase GetDatabase(string name);

        string Charset { get; set; }

        bool AutoCreateDatabase { get; set; }

        bool AutoCreateTable { get; set; }

        bool AutoCompareTableColumn { get; set; }

        DataBaseType DbType { get; }
    }
}
