﻿using System.Collections.Generic;
using System.Data;

namespace Dapper.Sharding
{
    public interface ITable<T>
    {
        string Name { get; }

        IDatabase DataBase { get; }

        DapperEntity DpEntity { get; }

        SqlFieldEntity SqlField { get; }

        ITable<T> BeginTran(IDbConnection conn, IDbTransaction tran, int? commandTimeout = null);

        bool Insert(T model);

        bool InsertIdentity(T model);

        bool InsertIfNoExists(T model);

        bool InsertIfExistsUpdate(T model, string fields = null);

        bool InsertIdentityIfNoExists(T model);

        bool InsertIdentityIfExistsUpdate(T model, string fields = null);

        bool Update(T model);

        bool UpdateInclude(T model, string fields);

        bool UpdateExclude(T model, string fields);

        int UpdateByWhere(T model, string where);

        int UpdateByWhereInclude(T model, string where, string fields);

        int UpdateByWhereExclude(T model, string where, string fields);

        bool Delete(object id);

        bool Delete(T model);

        int DeleteByIds(object ids);

        int DeleteByWhere(string where, object param = null);

        int DeleteAll();

        bool Exists(object id);

        bool Exists(T model);

        long Count();

        long Count(string where, object param = null);
    }
}
