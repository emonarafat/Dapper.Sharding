using System;
using System.Collections.Generic;
using System.Data;

namespace Dapper.Sharding
{
    public interface IDatabase
    {
        string Name { get; }

        IClient Client { get; }

        IDbConnection GetConn();

        void Using(Action<IDbConnection> action);

        void UsingTran(Action<IDbConnection, IDbTransaction> action);

        void SetCharset(string chartset);

        void CreateTable<T>(string name);

        void DropTable(string name);

        IEnumerable<string> GetTables();

        IEnumerable<dynamic> GetTableStatus();

        List<TableEntity> GetTableEnitys();

        ITableManager GetTableManager(string name);

        ITable<T> GetTable<T>(string name, IDbConnection conn, IDbTransaction tran = null, int? commandTimeout = null);

        string CreateTableScript<T>(string tableName);

    }
}
