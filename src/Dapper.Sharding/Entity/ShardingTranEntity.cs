using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dapper.Sharding
{
    internal class ShardingTranEntity<T>
    {
        public IDbConnection Conn { get; set; }

        public IDbTransaction Tran { get; set; }

        public ITable<T> Table { get; set; }

    }
}
