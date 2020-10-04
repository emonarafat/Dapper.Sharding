﻿using System;

namespace Dapper.Sharding
{
    internal class CsharpTypeToDbType
    {

        public static string Create(DataBaseType dbType,Type type, double length = 0)
        {
            switch (dbType)
            {
                case DataBaseType.MySql:return CreateMySqlType(type, length);
                case DataBaseType.Sqlite: return CreateSqliteType(type);
                case DataBaseType.SqlServer2008: return CreateSqlServerType(type, length);
                case DataBaseType.SqlServer2012: return CreateSqlServerType(type, length);
                case DataBaseType.Postgresql: return CreatePostgresqlType(type, length);
                case DataBaseType.Oracle: return CreateOracleType(type, length);
            }
            throw new Exception("no found");
        }

        private static string CreateSqlServerType(Type type, double length = 0)
        {
            if (type == typeof(Guid))
            {
                if (length <= 0)
                {
                    length = 36;
                }
                return $"varchar({length})";
            }

            if (type == typeof(string))
            {
                if (length <= -1)
                    return "nvarchar(max)";
                if (length == 0)
                    length = 24;
                return $"nvarchar({length})";
            }


            if (type == typeof(bool))
            {
                return "bit";
            }

            if (type == typeof(byte))
            {
                return "tinyint";
            }

            if (type == typeof(short) || type == typeof(ushort))
            {
                return "smallint";
            }

            if (type == typeof(int) || type == typeof(uint))
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

        private static string CreateMySqlType(Type type, double length = 0)
        {
            if (type == typeof(Guid))
            {
                if (length <= 0)
                {
                    length = 36;
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
                    length = 24;
                return $"varchar({length})";

            }

            if (type == typeof(bool))
            {
                return "bit(1)";
            }

            if (type == typeof(byte))
            {
                return "tinyint(4)";
            }

            if (type == typeof(short) || type == typeof(ushort))
            {
                return "smallint(6)";
            }

            if (type == typeof(int) || type == typeof(uint))
            {
                return "int(11)";
            }

            if (type == typeof(long) || type == typeof(ulong))
            {
                return "bigint(20)";
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

        private static string CreateSqliteType(Type type)
        {
            if (type == typeof(Guid))
            {
                return "TEXT";
            }

            if (type == typeof(string))
            {
                return "TEXT";
            }

            if (type == typeof(bool))
            {
                return "NUMERIC";
            }

            if (type == typeof(byte))
            {
                return "NUMERIC";
            }

            if (type == typeof(short) || type == typeof(ushort))
            {
                return "NUMERIC";
            }

            if (type == typeof(int) || type == typeof(uint))
            {
                return "NUMERIC";
            }

            if (type == typeof(long) || type == typeof(ulong))
            {
                return "NUMERIC";
            }

            if (type == typeof(float))
            {
                return "NUMERIC";
            }

            if (type == typeof(double))
            {
                return "NUMERIC";
            }

            if (type == typeof(decimal))
            {
                return "NUMERIC";
            }

            if (type == typeof(DateTime))
            {
                return "DATETIME";
            }

            return "BLOB";
        }

        private static string CreatePostgresqlType(Type type, double length = 0)
        {
            if (type == typeof(Guid))
            {
                if (length <= 0)
                {
                    length = 36;
                }
                return $"varchar({length})";

            }

            if (type == typeof(string))
            {
                if (length <= -1)
                    return "text";
                if (length == 0)
                    length = 24;
                return $"varchar({length})";

            }

            if (type == typeof(bool))
            {
                return "bool";
            }

            if (type == typeof(byte))
            {
                return "int2";
            }

            if (type == typeof(short) || type == typeof(ushort))
            {
                return "int2";
            }

            if (type == typeof(int) || type == typeof(uint))
            {
                return "int4";
            }

            if (type == typeof(long) || type == typeof(ulong))
            {
                return "int8";
            }

            if (type == typeof(float))
            {
                return "float4";
            }

            if (type == typeof(double))
            {
                return "float8";
            }

            if (type == typeof(decimal))
            {
                var len = length.ToString();
                if (len.Contains("."))
                {
                    len = len.Replace(".", ",");
                    return $"numeric({len})";
                }
                if (length <= 0)
                    return "numeric(18,2)";
                return $"numeric({length},0)";
            }

            if (type == typeof(DateTime))
            {
                return "timestamp";
            }

            return "bytea";

        }

        private static string CreateOracleType(Type type, double length = 0)
        {
            if (type == typeof(Guid))
            {
                if (length <= 0)
                {
                    length = 36;
                }
                return $"VARCHAR2({length})";

            }

            if (type == typeof(string))
            {
                if (length <= -1)
                    return "CLOB";
                if (length == 0)
                    length = 24;
                return $"VARCHAR2({length})";

            }

            if (type == typeof(bool))
            {
                return "NUMBER(1)";
            }

            if (type == typeof(byte))
            {
                return "NUMBER(4)";
            }

            if (type == typeof(short) || type == typeof(ushort))
            {
                return "NUMBER(4)";
            }

            if (type == typeof(int) || type == typeof(uint))
            {
                return "NUMBER(9)";
            }

            if (type == typeof(long) || type == typeof(ulong))
            {
                return "NUMBER(19)";
            }

            if (type == typeof(float))
            {
                return "NUMBER(7,3)";
            }

            if (type == typeof(double))
            {
                return "NUMBER(15,5)";
            }

            if (type == typeof(decimal))
            {
                var len = length.ToString();
                if (len.Contains("."))
                {
                    len = len.Replace(".", ",");
                    return $"NUMBER({len})";
                }
                if (length <= 0)
                    return "NUMBER(18,2)";
                return $"NUMBER({length},0)";
            }

            if (type == typeof(DateTime))
            {
                return "TIMESTAMP";
            }

            return "BLOB";
        }

    }
}
