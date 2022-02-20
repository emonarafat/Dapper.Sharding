#if CORE6
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dapper.Sharding
{
    internal class DateOnlyTypeHandler : SqlMapper.TypeHandler<DateOnly>
    {
        public override DateOnly Parse(object value)
        {
            return DateOnly.FromDayNumber((int)value);
        }

        public override void SetValue(IDbDataParameter parameter, DateOnly value)
        {
            parameter.DbType = DbType.Int32;
            parameter.Value = value.DayNumber;
        }
    }
}
#endif