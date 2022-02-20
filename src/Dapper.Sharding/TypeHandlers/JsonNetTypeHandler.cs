using Newtonsoft.Json;
using System;
using System.Data;

namespace Dapper.Sharding
{
    internal class JsonNetTypeHandler<T> : SqlMapper.TypeHandler<T>
    {
        public override T Parse(object value)
        {
            if (value == null || value == DBNull.Value)
            {
                return default;
            }
            try
            {
                var val = (string)value;
                if (val == "")
                {
                    return default;
                }
                return JsonConvert.DeserializeObject<T>(val);
            }
            catch
            {
                return default;
            }
        }

        public override void SetValue(IDbDataParameter parameter, T value)
        {
            parameter.DbType = DbType.String;
            if (value == null)
            {
                parameter.Value = DBNull.Value;
            }
            else
            {
                parameter.Value = JsonConvert.SerializeObject(value);

            }
        }
    }
}
