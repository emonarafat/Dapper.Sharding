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

        IDbConnection Conn { get; }

        IDbTransaction Tran { get; }

        int? CommandTimeout { get; }

        int Insert(T model);

        int Update(T model);
    }
}
