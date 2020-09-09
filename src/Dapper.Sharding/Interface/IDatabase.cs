using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;

namespace Dapper.Sharding
{
    public interface IDatabase
    {
        string Name { get; }

        LockManager Locker { get; }

        ConcurrentDictionary<string, object> TableCache { get; }

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

        TableEntity StatusToTableEntity(dynamic data);

        ITableManager GetTableManager(string name);

        TableEntity GetTableEntityFromDatabase(string name);

        List<TableEntity> GetTableEnityListFromDatabase();

        void GeneratorClassFile(string savePath, string tableName = "*", string nameSpace = "Model", string Suffix = "Table", bool partialClass = false);

        void CompareTableColumn<T>(string name, IEnumerable<string> dbColumns);

        ITable<T> GetTable<T>(string name);

    }
}
