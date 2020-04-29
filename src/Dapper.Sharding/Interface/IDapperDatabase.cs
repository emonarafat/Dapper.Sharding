using System;
using System.Collections.Generic;
using System.Data;

namespace Dapper.Sharding
{
    public interface IDapperDatabase
    {
        string Name { get; }

        IDapperClient Client { get; }

        IDbConnection GetConn();

        void Using(Action<IDbConnection> action);

        T Using<T>(Func<IDbConnection, T> func);

        void UsingTran(Action<IDbConnection, IDbTransaction> action);

        T UsingTran<T>(Func<IDbConnection, IDbTransaction, T> func);

        void SetCharset(string chartset);

        void CreateTable<T>(string name);

        void DropTable(string name);

        IEnumerable<string> GetTables();

        IEnumerable<dynamic> GetTableStatus();

        List<TableEntity> GetTableEnitys();

        IDapperTableManager GetTableManager(string name);

        IDapperTable<T> GetTable<T>(string name, IDbConnection conn, IDbTransaction tran = null, int? commandTimeout = null);

        string CreateTableScript<T>(string tableName);

    }
}
