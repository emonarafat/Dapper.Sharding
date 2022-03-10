using System;
using System.Reflection;

namespace Dapper.Sharding
{
    public class TypeHandlerJsonNet
    {
        public static void Add(Type type)
        {
            TypeHandlerCache.Add(type, () =>
            {
                SqlMapper.AddTypeHandler(type, new JsonNetTypeHandler());
            });
        }

        public static void Add(Assembly assembly)
        {
            TypeHandlerCache.GetJsonPropertyType(assembly, (type) =>
            {
                Add(type);
            });
        }
    }
}
