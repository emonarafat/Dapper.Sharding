using System;
using System.Collections.Generic;
using System.Text;

namespace Dapper.Sharding
{
    public abstract partial class ITable<T> where T : class
    {
        public abstract bool DeleteAsync(object id);
    }
}
