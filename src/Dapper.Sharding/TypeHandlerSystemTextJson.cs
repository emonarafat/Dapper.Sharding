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

        public static void AddCurrentDomain()
        {
            var assamblys = AppDomain.CurrentDomain.GetAssemblies();
            foreach (var item in assamblys)
            {
                Add(item);
            }
        }
    }
}
#endif

//Assembly.GetExecutingAssembly()
//typeof(T).Assembly
//Assembly.Load()
//AppDomain.CurrentDomain.GetAssemblies()