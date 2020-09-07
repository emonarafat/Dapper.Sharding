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

        IDbConnection Conn { get; set; }

        IDbTransaction Tran { get; set; }

        SqlFieldEntity SqlField { get; }

        int? CommandTimeout { get; set; }

        bool Insert(T model);

        bool Update(T model);

        int UpdateByWhere(T model, string where, string updateFields);

        bool Delete(object id);

        bool Delete(T model);
    }
}
