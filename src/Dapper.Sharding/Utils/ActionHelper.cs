using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dapper.Sharding
{
    internal static class ActionHelper
    {
        public static void Using<T>(this ITable<T> table, Action action)
        {
            if (table.Conn == null)
            {
                using (table.Conn = table.DataBase.GetConn())
                {
                    action();
                    table.Conn = null;
                }
            }
            else
            {
                action();
            }
        }

        public static T Using<T, T2>(this ITable<T2> table, Func<T> action)
        {
            if (table.Conn == null)
            {
                using (table.Conn = table.DataBase.GetConn())
                {
                    var result = action();
                    table.Conn = null;
                    return result;
                }
            }
            else
            {
                return action();
            }
        }
    }
}
