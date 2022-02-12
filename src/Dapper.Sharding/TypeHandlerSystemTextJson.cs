#if CORE
using System;
using System.Data;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.Unicode;

namespace Dapper.Sharding
{
    public class TypeHandlerSystemTextJson
    {
        public static void Add<T>()
        {
            SqlMapper.AddTypeHandler(new SystemTextJsonTypeHandler<T>());
        }
    }

    internal class SystemTextJsonTypeHandler<T> : SqlMapper.TypeHandler<T>
    {
        private static readonly JsonSerializerOptions options = new JsonSerializerOptions()
        {
            Encoder = JavaScriptEncoder.Create(UnicodeRanges.All), //不编码中文
            NumberHandling = JsonNumberHandling.AllowReadingFromString, //反序列化时允许string转数字
            PropertyNameCaseInsensitive = true //忽略大小写
        };

        public override T Parse(object value)
        {
            if (value == null || value == DBNull.Value)
            {
                return default;
            }
            try
            {
                return JsonSerializer.Deserialize<T>((string)value, options);
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
                parameter.Value = JsonSerializer.Serialize(value, options);
            }
        }
    }
}
#endif