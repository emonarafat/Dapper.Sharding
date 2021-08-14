using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Dapper.Sharding
{
    internal class ClickHouseTableManager : ITableManager
    {
        public ClickHouseTableManager(string name, IDatabase database) : base(name, database)
        {

        }

        public override void CreateIndex(string name, string columns, IndexType indexType)
        {
            throw new NotImplementedException();
        }


        public override void DropIndex(string name)
        {
            throw new NotImplementedException();
        }


        public override void AddColumn(string name, Type t, double length = 0, string comment = null)
        {
            var dbType = CsharpTypeToDbType.Create(DataBase.DbType, t, length);
            if (t.IsValueType && t != typeof(DateTime) && t != typeof(DateTimeOffset))
            {
                dbType += " DEFAULT 0";
            }
            DataBase.Execute($"ALTER TABLE `{Name}` ADD COLUMN IF NOT EXISTS `{name}` {dbType} COMMENT '{comment}'");
        }

        public override void DropColumn(string name)
        {
            DataBase.Execute($"ALTER TABLE `{Name}` DROP COLUMN IF EXISTS `{name}`");
        }

        public override void ModifyColumn(string name, Type t, double length = 0, string comment = null)
        {
            var dbType = CsharpTypeToDbType.Create(DataBase.DbType, t, length);
            DataBase.Execute($"ALTER TABLE `{Name}` MODIFY COLUMN IF EXISTS `{name}` {dbType}");
        }

        public override List<IndexEntity> GetIndexEntityList()
        {
            throw new NotImplementedException();
        }

        public override List<ColumnEntity> GetColumnEntityList(TableEntity tb = null)
        {
            var list = new List<ColumnEntity>();
            var columnList = DataBase.Query($"DESCRIBE TABLE {Name}");
            foreach (var item in columnList)
            {
                var model = new ColumnEntity();
                model.Name = item.name; //列名
                model.Comment = item.comment; //说明
                string columnType = item.type;//数据类型
                if (columnType == "String")
                {
                    model.CsStringType = "string";
                    model.CsType = typeof(string);
                }
                else if (columnType.StartsWith("FixedString"))
                {
                    model.CsStringType = "string";
                    model.CsType = typeof(string);
                    var len = columnType.Split('(')[1].Split(')')[0];
                    model.Length = Convert.ToInt32(len);
                }
                else if (columnType == "UUID")
                {
                    model.CsStringType = "Guid";
                    model.CsType = typeof(Guid);
                }
                else if (columnType == "DateTime")
                {
                    model.CsStringType = "DateTime";
                    model.CsType = typeof(DateTime);
                }
                else if (columnType == "Date")
                {
                    model.CsStringType = "DateTime";
                    model.CsType = typeof(DateTime);
                    model.Length = -1;
                }
                else if (columnType == "DateTime64")
                {
                    model.CsStringType = "DateTime";
                    model.CsType = typeof(DateTime);
                    model.Length = -2;
                }
                else if (columnType == "Float32")
                {
                    model.CsStringType = "float";
                    model.CsType = typeof(float);
                }
                else if (columnType == "Float64")
                {
                    model.CsStringType = "double";
                    model.CsType = typeof(double);
                }
                else if (columnType == "Int8")
                {
                    model.CsStringType = "sbyte";
                    model.CsType = typeof(sbyte);
                }
                else if (columnType == "Int16")
                {
                    model.CsStringType = "short";
                    model.CsType = typeof(short);
                }
                else if (columnType == "Int32")
                {
                    model.CsStringType = "int";
                    model.CsType = typeof(int);
                }
                else if (columnType == "Int64")
                {
                    model.CsStringType = "long";
                    model.CsType = typeof(long);
                }
                else if (columnType == "UInt8")
                {
                    model.CsStringType = "byte";
                    model.CsType = typeof(byte);
                }
                else if (columnType == "UInt16")
                {
                    model.CsStringType = "ushort";
                    model.CsType = typeof(ushort);
                }
                else if (columnType == "UInt32")
                {
                    model.CsStringType = "uint";
                    model.CsType = typeof(uint);
                }
                else if (columnType == "UInt64")
                {
                    model.CsStringType = "ulong";
                    model.CsType = typeof(ulong);
                }
                else if (columnType.StartsWith("Decimal32") || columnType.StartsWith("Decimal("))
                {
                    model.CsStringType = "decimal";
                    model.CsType = typeof(decimal);
                    try
                    {
                        var len = columnType.Split('(')[1].Split(')')[0];
                        model.Length = Convert.ToDouble("18." + len);
                    }
                    catch { }
                }
                else if (columnType.StartsWith("Decimal64"))
                {
                    model.CsStringType = "decimal";
                    model.CsType = typeof(decimal);
                    try
                    {
                        var len = columnType.Split('(')[1].Split(')')[0];
                        model.Length = Convert.ToDouble("18." + len);
                    }
                    catch { }
                }
                else if (columnType.StartsWith("Decimal128"))
                {
                    model.CsStringType = "decimal";
                    model.CsType = typeof(decimal);
                    try
                    {
                        var len = columnType.Split('(')[1].Split(')')[0];
                        model.Length = Convert.ToDouble("18." + len);
                    }
                    catch { }
                }

                list.Add(model);
            }
            return list;
        }
    }
}
