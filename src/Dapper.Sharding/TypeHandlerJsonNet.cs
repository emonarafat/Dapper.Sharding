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
