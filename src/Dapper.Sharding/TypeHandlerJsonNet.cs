using Newtonsoft.Json;
using System;
using System.Data;

namespace Dapper.Sharding
{
    public class TypeHandlerJsonNet
    {
        public static void Add<T>()
        {
            SqlMapper.AddTypeHandler(new JsonNetTypeHandler<T>());
        }
    }

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
                return JsonConvert.DeserializeObject<T>((string)value);
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
