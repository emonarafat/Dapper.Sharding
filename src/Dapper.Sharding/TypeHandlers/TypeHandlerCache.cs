﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Dapper.Sharding
{
    internal class TypeHandlerCache
    {
        private static readonly object _locker = new object();
        private static readonly HashSet<RuntimeTypeHandle> hash = new HashSet<RuntimeTypeHandle>();

        internal static void Add(Type t, Action action)
        {
            if (!hash.Contains(t.TypeHandle))
            {
                lock (_locker)
                {
                    if (!hash.Contains(t.TypeHandle))
                    {
                        hash.Add(t.TypeHandle);
                        action();
                    }
                }
            }
        }

        internal static void Add(Assembly assembly, Action<Type> action)
        {
            var types = assembly.GetTypes();
            foreach (var type in types)
            {
                var tableAttr = type.GetCustomAttributes(false).FirstOrDefault(f => f is TableAttribute);
                if (tableAttr != null)
                {
                    var pros = type.GetProperties();
                    foreach (var pro in pros)
                    {
                        var colAttr = pro.GetCustomAttributes(false).FirstOrDefault(f => f is ColumnAttribute) as ColumnAttribute;
                        if (colAttr != null)
                        {
                            if (colAttr.ColumnType == "json" || colAttr.ColumnType == "jsonb" || colAttr.ColumnType == "jsons")
                            {
                                if (pro.PropertyType != typeof(string))
                                {
                                    action(pro.PropertyType);
                                }
                            }
                        }
                    }
                }
            }

        }

    }
}
