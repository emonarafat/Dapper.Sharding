#if CORE
using System;
using System.Data;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.Unicode;

namespace Dapper.Sharding
{

    internal class SystemTextJsonTypeHandler<T> : SqlMapper.TypeHandler<T>
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
                return JsonSerializer.Deserialize<T>(val, TextJsonOptions.Options);
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
                parameter.Value = JsonSerializer.Serialize(value, TextJsonOptions.Options);
            }
        }
    }
}
#endif