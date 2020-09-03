﻿using System;
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

        IEnumerable<string> ShowTables();

        bool ExistsTable(string name);

        string ShowTableScript<T>(string name);

        IEnumerable<dynamic> ShowTableStatus();

        List<TableEntity> GetTableEnitys();

        ITableManager GetTableManager(string name);

        ITable<T> GetTable<T>(string name, IDbConnection conn, IDbTransaction tran = null, int? commandTimeout = null);

    }
}
