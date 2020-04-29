using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dapper.Sharding
{
    public class CsharpTypeToDbType
    {

        public static string CreateSqlServer(Type type, double length = 0)
        {

            if (type == typeof(Guid))
            {
                return "uniqueidentifier";
            }

            if (type == typeof(string))
            {
                if (length <= -1)
                    return "nvarchar(max)";
                if (length == 0)
                    length = 50;
                return $"nvarchar({length})";
            }

            if (type == typeof(int) || type == typeof(uint) || type == typeof(short) || type == typeof(ushort))
            {
                return "int";
            }

            if (type == typeof(long) || type == typeof(ulong))
            {
                return "bigint";
            }

            if (type == typeof(float))
            {
                return "real";
            }

            if (type == typeof(double))
            {
                return "float";
            }

            if (type == typeof(decimal))
            {
                var len = length.ToString();
                if (len.Contains("."))
                {
                    len = len.Replace(".", ",");
                    return $"decimal({len})";
                }
                if (length <= 0)
                    return "decimal(18,2)";
                return $"decimal({length},0)";
            }

            if (type == typeof(bool))
            {
                return "bit";
            }

            if (type == typeof(DateTime))
            {
                if (length == 0)
                    return "datetime";
                if (length == -1)
                    return "date";
                if (length == -2)
                    return "timestamp";
                return $"datetimeoffset({length})";
            }

            if (length <= 0)
                length = 50;
            return $"binary({length})";
        }

        public static string CreateMySqlType(Type type, double length = 0)
        {
            if (type == typeof(Guid))
            {
                if (length <= 0)
                {
                    length = 32;
                }
                return $"varchar({length})";

            }

            if (type == typeof(string))
            {
                if (length == -1)
                    return "text";
                if (length == -2)
                    return "longtext";
                if (length == 0)
                    length = 50;
                return $"varchar({length})";

            }

            if (type == typeof(int) || type == typeof(uint) || type == typeof(short) || type == typeof(ushort))
            {
                if (length <= 0)
                    length = 11;
                return $"int({length})";
            }

            if (type == typeof(long) || type == typeof(ulong))
            {
                if (length <= 0)
                    length = 20;
                return $"bigint({length})";
            }

            if (type == typeof(float))
            {
                return "float";
            }

            if (type == typeof(double))
            {
                return "double";
            }

            if (type == typeof(decimal))
            {
                var len = length.ToString();
                if (len.Contains("."))
                {
                    len = len.Replace(".", ",");
                    return $"decimal({len})";
                }
                if (length <= 0)
                    return "decimal(18,2)";
                return $"decimal({length},0)";
            }

            if (type == typeof(bool))
            {
                return "bit(1)";
            }

            if (type == typeof(DateTime))
            {
                return "datetime";
            }

            if (length >= 0)
                return "blob";
            if (length == -1)
                return "tinyblob";
            if (length == -2)
                return "mediumblob";
            return "longblob";

        }

        public static string CreateSqliteType(Type type)
        {
            if (type == typeof(Guid))
            {
                return "TEXT";
            }

            if (type == typeof(string))
            {
                return "TEXT";
            }

            if (type == typeof(int) || type == typeof(uint) || type == typeof(short) || type == typeof(ushort))
            {
                return "INTEGER";
            }

            if (type == typeof(long) || type == typeof(ulong))
            {
                return "INTEGER";
            }

            if (type == typeof(float))
            {
                return "real";
            }

            if (type == typeof(double))
            {
                return "real";
            }

            if (type == typeof(decimal))
            {
                return "real";
            }

            if (type == typeof(bool))
            {
                return "INTEGER";
            }

            return "blob";
        }

        public static string CreatePostgresqlType(Type type, double length)
        {
            return null;
        }

        public static string CreateOracleType(Type type, double length)
        {
            return null;
        }

    }
}
