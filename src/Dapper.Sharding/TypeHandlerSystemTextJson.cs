#if CORE
using System;
using System.Reflection;

namespace Dapper.Sharding
{
    public class TypeHandlerSystemTextJson
    {
        public static void Add(Type type)
        {
            TypeHandlerCache.Add(type, () =>
            {
                SqlMapper.AddTypeHandler(type, new SystemTextJsonTypeHandler());
            });

        }

        public static void Add<T>()
        {
            Add(typeof(T));
        }

        public static void Add(Assembly assembly)
        {
            TypeHandlerCache.Add(assembly, (type) =>
            {
                Add(type);
            });
        }
    }
}
#endif