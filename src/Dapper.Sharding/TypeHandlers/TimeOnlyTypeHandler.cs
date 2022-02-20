#if CORE6
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dapper.Sharding
{
    internal class TimeOnlyTypeHandler : SqlMapper.TypeHandler<TimeOnly>
    {
        public override TimeOnly Parse(object value)
        {
            return new TimeOnly((long)value);
        }

        public override void SetValue(IDbDataParameter parameter, TimeOnly value)
        {
            parameter.DbType = DbType.Int64;
            parameter.Value = value.Ticks;
        }
    }
}
#endif