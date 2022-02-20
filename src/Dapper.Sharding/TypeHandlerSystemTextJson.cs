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
}
#endif