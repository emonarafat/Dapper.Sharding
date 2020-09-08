using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dapper.Sharding
{
    public interface ITable<T>
    {
        string Name { get; }

        IDatabase DataBase { get; }

        IDbConnection Conn { get; }

        IDbTransaction Tran { get; }

        int? CommandTimeout { get; }

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

        bool Exists(object id);

        bool Exists(T model);
    }
}
