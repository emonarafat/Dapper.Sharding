using System;
using System.Collections.Generic;
using System.Data;

namespace Dapper.Sharding
{
    public interface IDatabase
    {
        string Name { get; }

        LockManager Locker { get; }

        HashSet<string> TableCache { get; }

        IClient Client { get; }

        IDbConnection GetConn();

        void Using(Action<IDbConnection> action);

        void UsingTran(Action<IDbConnection, IDbTransaction> action);

        void SetCharset(string chartset);

        void CreateTable<T>(string name);

        void DropTable(string name);

        void TruncateTable(string name);

        IEnumerable<string> ShowTableList();

        bool ExistsTable(string name);

        string ShowTableScript<T>(string name);

        dynamic ShowTableStatus(string name);

        IEnumerable<dynamic> ShowTableStatusList();

        ITableManager GetTableManager(string name, IDbConnection conn = null, IDbTransaction tran = null, int? commandTimeout = null);

        TableEntity GetTableEntityFromDatabase(string name);

        List<TableEntity> GetTableEnityListFromDatabase();

        void GeneratorClassFile(string savePath, string tableName = "*", string nameSpace = "Model", string Suffix = "Table", bool partialClass = false);

        ITable<T> GetTable<T>(string name, IDbConnection conn = null, IDbTransaction tran = null, int? commandTimeout = null);

    }
}
