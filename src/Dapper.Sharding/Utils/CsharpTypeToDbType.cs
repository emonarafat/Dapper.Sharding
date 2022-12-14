using System;
using System.Collections.Generic;

namespace Dapper.Sharding
{
    internal class CsharpTypeToDbType
    {

        public static string Create(DataBaseType dbType, Type type, double length = 0, string columnType = null)
        {
            if (!string.IsNullOrEmpty(columnType))
            {
                if (columnType == "jsons")
                {
                    if (dbType == DataBaseType.MySql)
                    {
                        return "longtext";
                    }
                    if (dbType == DataBaseType.Postgresql)
                    {
                        return "text";
                    }
                    if (dbType == DataBaseType.Sqlite)
                    {
                        return "text";
                    }
                    if (dbType == DataBaseType.ClickHouse)
                    {
                        return "String";
                    }
                    if (dbType == DataBaseType.Oracle)
                    {
                        return "CLOB";
                    }
                    return "nvarchar(max)"; //sqlserver
                }

                if (dbType != DataBaseType.Postgresql)
                {
                    if (columnType == "json" || columnType == "jsonb")
                    {
                        if (dbType == DataBaseType.MySql)
                        {
                            return "json";
                        }
                        else if (dbType == DataBaseType.SqlServer2012 || dbType == DataBaseType.SqlServer2008 || dbType == DataBaseType.SqlServer2005)
                        {
                            return "nvarchar(max)";
                        }
                        else if (dbType == DataBaseType.ClickHouse)
                        {
                            return "String";
                        }
                        else if (dbType == DataBaseType.Oracle)
                        {
                            return "CLOB";
                        }
                        else
                        {
                            return "text";
                        }
                    }
                }

                if (dbType != DataBaseType.MySql)
                {
                    if (columnType == "longtext")
                    {
                        if (dbType == DataBaseType.SqlServer2012 || dbType == DataBaseType.SqlServer2008 || dbType == DataBaseType.SqlServer2005)
                        {
                            return "nvarchar(max)";
                        }
                        else if (dbType == DataBaseType.Oracle)
                        {
                            return "CLOB";
                        }
                        else if (dbType == DataBaseType.ClickHouse)
                        {
                            return "String";
                        }
                        else
                        {
                            return "text";
                        }
                    }
                }
                return columnType;
            }
            switch (dbType)
            {
                case DataBaseType.MySql: return CreateMySqlType(type, length);
                case DataBaseType.Sqlite: return CreateSqliteType(type);
                case DataBaseType.SqlServer2005: return CreateSqlServerType(type, length);
                case DataBaseType.SqlServer2008: return CreateSqlServerType(type, length);
                case DataBaseType.SqlServer2012: return CreateSqlServerType(type, length);
                case DataBaseType.Postgresql: return CreatePostgresqlType(type, length);
                case DataBaseType.Oracle: return CreateOracleType(type, length);
                case DataBaseType.ClickHouse: return CreateClickHouseType(type, length);
            }
            throw new Exception("CsharpTypeToDbType no found");
        }

        private static string UnknownTypeMessage(string t)
        {
            return $"unknown type {t}, please set ColumnAttribute ColumnType like [Column(columnType:\"jsonb\")] or other database type.";
        }

        private static string CreateSqlServerType(Type type, double length = 0)
        {
            if (type == typeof(Guid))
            {
                if (length <= 0)
                {
                    length = 36;
                }
                return $"nvarchar({length})";
            }

            if (type == typeof(string))
            {
                if (length == -2)
                    return "text";
                if (length == -3)
                    return "ntext";
                if (length <= -1)
                    return "nvarchar(max)";
                if (length == 0)
                    length = 50;
                return $"nvarchar({length})";
            }

            if (type == typeof(bool))
            {
                return "bit";
            }

            if (type == typeof(byte) || type == typeof(sbyte))
            {
                return "tinyint";
            }

            if (type == typeof(short) || type == typeof(ushort))
            {
                return "smallint";
            }

            if (type == typeof(int) || type.BaseType == typeof(Enum) || type == typeof(uint))
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
                if (length >= 0)
                {
                    if (length > 7)
                    {
                        length = 7;
                    }
                    return $"datetime2({length})";
                }
                if (length == -1)
                    return "date";
                if (length == -2)
                    return "timestamp";
                return $"datetime";
            }

            if (type == typeof(DateTimeOffset))
            {
                if (length >= 0)
                {
                    if (length > 7)
                    {
                        length = 7;
                    }
                }
                return $"datetimeoffset({length})";
            }

#if CORE6
            if (type == typeof(DateOnly))
            {
                if (ShardingFactory.DateOnlyFormat == DbTypeDateOnly.Date)
                {
                    return "date";
                }
                else if (ShardingFactory.DateOnlyFormat == DbTypeDateOnly.DateTime)
                {
                    return "datetime2(0)";
                }
                else if (ShardingFactory.DateOnlyFormat == DbTypeDateOnly.Number)
                {
                    return "int";
                }
                else if (ShardingFactory.DateOnlyFormat == DbTypeDateOnly.String)
                {
                    return "varchar(10)";
                }
                return "int";
            }

            if (type == typeof(TimeOnly))
            {
                if (ShardingFactory.TimeOnlyFormat == DbTypeTimeOnly.TimeSpan)
                {
                    return "time";
                }
                else if (ShardingFactory.TimeOnlyFormat == DbTypeTimeOnly.Time)
                {
                    return "time";
                }
                else if (ShardingFactory.TimeOnlyFormat == DbTypeTimeOnly.DateTime)
                {
                    return "datetime2(7)";
                }
                else if (ShardingFactory.TimeOnlyFormat == DbTypeTimeOnly.Number)
                {
                    return "bigint";
                }
                else if (ShardingFactory.TimeOnlyFormat == DbTypeTimeOnly.String)
                {
                    return "varchar(8)";
                }
                return "bigint";
            }
#endif

            throw new Exception(UnknownTypeMessage(type.Name));

            //if (length <= 0)
            //    length = 50;
            //return $"binary({length})";
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
                    return "longtext";
                if (length == -2)
                    return "text";
                if (length == -3)
                    return "mediumtext";
                if (length == -4)
                    return "tinytext";
                if (length <= 0)
                    length = 50;
                return $"varchar({length})";
            }

            if (type == typeof(bool))
            {
                return "bit(1)";
            }

            if (type == typeof(byte) || type == typeof(sbyte))
            {
                return "tinyint(4)";
            }

            if (type == typeof(short) || type == typeof(ushort))
            {
                return "smallint(6)";
            }

            if (type == typeof(int) || type.BaseType == typeof(Enum) || type == typeof(uint))
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
                if (length == 0)
                    return "datetime";
                if (length > 0)
                {
                    if (length > 6)
                    {
                        length = 6;
                    }
                    return $"datetime({length})";
                }
                if (length == -1)
                    return "datetime2";
                if (length == -2)
                    return "date";
                if (length == -3)
                    return "smalldatetime";
                return "timestamp";

            }

            if (type == typeof(DateTimeOffset))
            {
                return "timestamp";
            }

#if CORE6
            if (type == typeof(DateOnly))
            {
                if (ShardingFactory.DateOnlyFormat == DbTypeDateOnly.Date)
                {
                    return "date";
                }
                else if (ShardingFactory.DateOnlyFormat == DbTypeDateOnly.DateTime)
                {
                    return "datetime";
                }
                else if (ShardingFactory.DateOnlyFormat == DbTypeDateOnly.Number)
                {
                    return "int(11)";
                }
                else if (ShardingFactory.DateOnlyFormat == DbTypeDateOnly.String)
                {
                    return "varchar(10)";
                }
                return "int(11)";
            }

            if (type == typeof(TimeOnly))
            {
                if (ShardingFactory.TimeOnlyFormat == DbTypeTimeOnly.TimeSpan)
                {
                    return "time";
                }
                else if (ShardingFactory.TimeOnlyFormat == DbTypeTimeOnly.Time)
                {
                    return "time";
                }
                else if (ShardingFactory.TimeOnlyFormat == DbTypeTimeOnly.DateTime)
                {
                    return "datetime(6)";
                }
                else if (ShardingFactory.TimeOnlyFormat == DbTypeTimeOnly.Number)
                {
                    return "bigint(20)";
                }
                else if (ShardingFactory.TimeOnlyFormat == DbTypeTimeOnly.String)
                {
                    return "varchar(8)";
                }
                return "bigint(20)";
            }
#endif

            throw new Exception(UnknownTypeMessage(type.Name));

            //if (length >= 0)
            //    return "blob";
            //if (length == -1)
            //    return "tinyblob";
            //if (length == -2)
            //    return "mediumblob";
            //return "longblob";

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

            if (type == typeof(byte) || type == typeof(sbyte))
            {
                return "NUMERIC";
            }

            if (type == typeof(short) || type == typeof(ushort))
            {
                return "NUMERIC";
            }

            if (type == typeof(int) || type.BaseType == typeof(Enum) || type == typeof(uint))
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

#if CORE6
            if (type == typeof(DateOnly))
            {
                if (ShardingFactory.DateOnlyFormat == DbTypeDateOnly.Date)
                {
                    return "DATE";
                }
                else if (ShardingFactory.DateOnlyFormat == DbTypeDateOnly.DateTime)
                {
                    return "DATETIME";
                }
                else if (ShardingFactory.DateOnlyFormat == DbTypeDateOnly.Number)
                {
                    return "NUMERIC";
                }
                else if (ShardingFactory.DateOnlyFormat == DbTypeDateOnly.String)
                {
                    return "TEXT";
                }
                return "NUMERIC";
            }

            if (type == typeof(TimeOnly))
            {
                if (ShardingFactory.TimeOnlyFormat == DbTypeTimeOnly.TimeSpan)
                {
                    return "DATETIME";
                }
                else if (ShardingFactory.TimeOnlyFormat == DbTypeTimeOnly.Time)
                {
                    return "TIME";
                }
                else if (ShardingFactory.TimeOnlyFormat == DbTypeTimeOnly.DateTime)
                {
                    return "DATETIME";
                }
                else if (ShardingFactory.TimeOnlyFormat == DbTypeTimeOnly.Number)
                {
                    return "NUMERIC";
                }
                else if (ShardingFactory.TimeOnlyFormat == DbTypeTimeOnly.String)
                {
                    return "TEXT";
                }
                return "NUMERIC";
            }
#endif

            throw new Exception(UnknownTypeMessage(type.Name));

            //return "BLOB";
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
                if (length == -1)
                    return "text";
                if (length == -10)
                    return "jsonb";
                if (length == -11)
                    return "json";
                if (length == -20)
                    return "geometry";
                if (length > -30 && length < -20)
                {
                    var str = length.ToString();
                    if (str.Contains("."))
                    {
                        var srid = str.Split('.')[1];
                        return $"geometry(geometry,{srid})";
                    }
                    else
                    {
                        return "geometry";
                    }
                }
                if (length <= 0)
                    length = 50;
                return $"varchar({length})";

            }

            if (type == typeof(bool))
            {
                return "bool";
            }

            if (type == typeof(byte) || type == typeof(sbyte))
            {
                return "int2";
            }

            if (type == typeof(short) || type == typeof(ushort))
            {
                return "int2";
            }

            if (type == typeof(int) || type.BaseType == typeof(Enum) || type == typeof(uint))
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
                if (length >= 0)
                {
                    if (length > 6)
                    {
                        length = 6;
                    }
                    return $"timestamp({length})";
                }
                if (length == -1)
                    return "timestamptz";
                return "date";
            }

            if (type == typeof(DateTimeOffset))
            {
                if (length > 6)
                {
                    length = 6;
                }
                return $"timetz({length})";
            }

            if (type == typeof(TimeSpan))
            {
                if (length >= 0)
                    return "time";
                return "interval";
            }

#if CORE6
            if (type == typeof(DateOnly))
            {
                if (ShardingFactory.DateOnlyFormat == DbTypeDateOnly.Date)
                {
                    return "date";
                }
                else if (ShardingFactory.DateOnlyFormat == DbTypeDateOnly.DateTime)
                {
                    return "timestamp(0)";
                }
                else if (ShardingFactory.DateOnlyFormat == DbTypeDateOnly.Number)
                {
                    return "int4";
                }
                else if (ShardingFactory.DateOnlyFormat == DbTypeDateOnly.String)
                {
                    return "varchar(10)";
                }
                return "int4";
            }

            if (type == typeof(TimeOnly))
            {
                if (ShardingFactory.TimeOnlyFormat == DbTypeTimeOnly.TimeSpan)
                {
                    return "time";
                }
                else if (ShardingFactory.TimeOnlyFormat == DbTypeTimeOnly.Time)
                {
                    return "timestamp(6)";
                }
                else if (ShardingFactory.TimeOnlyFormat == DbTypeTimeOnly.DateTime)
                {
                    return "timestamp(6)";
                }
                else if (ShardingFactory.TimeOnlyFormat == DbTypeTimeOnly.Number)
                {
                    return "int8";
                }
                else if (ShardingFactory.TimeOnlyFormat == DbTypeTimeOnly.String)
                {
                    return "varchar(8)";
                }
                return "int8";
            }
#endif

            var typeList = new List<string>
            {
                "Point", "MultiPoint", "LineString",
                "MultiLineString", "Polygon", "MultiPolygon", "GeometryCollection",
                "Feature","FeatureCollection","GeoJSONObject","Geometry"
            };

            if (typeList.Contains(type.Name))
            {
                return "geometry";
            }
            throw new Exception(UnknownTypeMessage(type.Name));

            //return "bytea";

        }

        private static string CreateOracleType(Type type, double length = 0)
        {
            if (type == typeof(Guid))
            {
                if (length <= 0)
                {
                    length = 36;
                }
                return $"NVARCHAR2({length})";

            }

            if (type == typeof(string))
            {
                if (length <= -1)
                    return "CLOB";
                if (length <= 0)
                    length = 50;
                return $"NVARCHAR2({length})";

            }

            if (type == typeof(bool))
            {
                return "NUMBER(1)";
            }

            if (type == typeof(byte) || type == typeof(sbyte))
            {
                return "NUMBER(4)";
            }

            if (type == typeof(short) || type == typeof(ushort))
            {
                return "NUMBER(4)";
            }

            if (type == typeof(int) || type.BaseType == typeof(Enum) || type == typeof(uint))
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

#if CORE6
            if (type == typeof(DateOnly))
            {
                if (ShardingFactory.DateOnlyFormat == DbTypeDateOnly.Date)
                {
                    return "DATE";
                }
                else if (ShardingFactory.DateOnlyFormat == DbTypeDateOnly.DateTime)
                {
                    return "TIMESTAMP";
                }
                else if (ShardingFactory.DateOnlyFormat == DbTypeDateOnly.Number)
                {
                    return "NUMBER(9)";
                }
                else if (ShardingFactory.DateOnlyFormat == DbTypeDateOnly.String)
                {
                    return "VARCHAR2(10)";
                }
                return "NUMBER(9)";
            }

            if (type == typeof(TimeOnly))
            {
                if (ShardingFactory.TimeOnlyFormat == DbTypeTimeOnly.TimeSpan)
                {
                    return "TIME";
                }
                else if (ShardingFactory.TimeOnlyFormat == DbTypeTimeOnly.Time)
                {
                    return "TIME";
                }
                else if (ShardingFactory.TimeOnlyFormat == DbTypeTimeOnly.DateTime)
                {
                    return "TIMESTAMP";
                }
                else if (ShardingFactory.TimeOnlyFormat == DbTypeTimeOnly.Number)
                {
                    return "NUMBER(19)";
                }
                else if (ShardingFactory.TimeOnlyFormat == DbTypeTimeOnly.String)
                {
                    return "VARCHAR2(8)";
                }
                return "NUMBER(19)";
            }
#endif

            throw new Exception(UnknownTypeMessage(type.Name));

            //return "BLOB";
        }

        private static string CreateClickHouseType(Type type, double length = 0)
        {
            if (type == typeof(Guid))
            {
                return $"UUID";
            }

            if (type == typeof(string))
            {
                if (length > 0 && ShardingFactory.ClickHouseFixedString)
                {
                    return $"FixedString({length})";
                }
                return "String";
            }

            if (type == typeof(DateTime))
            {
                if (length == -1)
                {
                    return "Date";
                }
                if (length == -2)
                {
                    return "Datetime64";
                }
                return "Datetime";
            }

            if (type == typeof(float))
            {
                return "Float32";
            }

            if (type == typeof(double))
            {
                return "Float64";
            }

            if (type == typeof(sbyte))
            {
                return "Int8";
            }

            if (type == typeof(short))
            {
                return "Int16";
            }

            if (type == typeof(int) || type.BaseType == typeof(Enum))
            {
                if (length == -1)
                {
                    return "Int16";
                }
                if (length == -2)
                {
                    return "Int8";
                }
                return "Int32";
            }

            if (type == typeof(long))
            {
                return "Int64";
            }

            if (type == typeof(byte))
            {
                return "UInt8";
            }

            if (type == typeof(ushort))
            {
                return "UInt16";
            }

            if (type == typeof(uint))
            {
                return "UInt32";
            }

            if (type == typeof(ulong))
            {
                return "UInt64";
            }

            if (type == typeof(decimal))
            {
                if (length <= 0)
                {
                    length = 18.2;
                }
                var p = 1;
                var s = 0;
                var len = length.ToString();
                if (len.Contains("."))
                {
                    var arr = len.Split('.');
                    p = Convert.ToInt32(arr[0]);
                    s = Convert.ToInt32(arr[1]);
                }
                else
                {
                    p = Convert.ToInt32(len);
                }

                if (p >= 1 && p <= 9)
                {
                    return $"Decimal32({s})";
                }

                if (p >= 10 && p <= 18)
                {
                    return $"Decimal64({s})";
                }

                if (p >= 19 && p <= 38)
                {
                    return $"Decimal128({s})";
                }

                return $"Decimal32({s})";
            }

#if CORE6
            if (type == typeof(DateOnly))
            {
                if (ShardingFactory.DateOnlyFormat == DbTypeDateOnly.Date)
                {
                    return "Date";
                }
                else if (ShardingFactory.DateOnlyFormat == DbTypeDateOnly.DateTime)
                {
                    return "Datetime";
                }
                else if (ShardingFactory.DateOnlyFormat == DbTypeDateOnly.Number)
                {
                    return "Int32";
                }
                else if (ShardingFactory.DateOnlyFormat == DbTypeDateOnly.String)
                {
                    return "String";
                }
                return "Int32";
            }

            if (type == typeof(TimeOnly))
            {
                if (ShardingFactory.TimeOnlyFormat == DbTypeTimeOnly.TimeSpan)
                {
                    return "Datetime64";
                }
                else if (ShardingFactory.TimeOnlyFormat == DbTypeTimeOnly.Time)
                {
                    return "Datetime64";
                }
                else if (ShardingFactory.TimeOnlyFormat == DbTypeTimeOnly.DateTime)
                {
                    return "Datetime64";
                }
                else if (ShardingFactory.TimeOnlyFormat == DbTypeTimeOnly.Number)
                {
                    return "Int64";
                }
                else if (ShardingFactory.TimeOnlyFormat == DbTypeTimeOnly.String)
                {
                    return "String";
                }
                return "Int64";
            }
#endif

            return "String";
        }
    }
}
