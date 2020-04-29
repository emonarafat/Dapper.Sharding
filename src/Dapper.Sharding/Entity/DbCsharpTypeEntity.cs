using System;

namespace Dapper.Sharding
{
    public class DbCsharpTypeEntity
    {
        public string DbType { get; set; }

        public string CsStringType { get; set; }

        public Type CsType
        {
            get
            {
                switch (CsStringType)
                {
                    case "Guid": return typeof(Guid);
                    case "string": return typeof(string);
                    case "int": return typeof(int);
                    case "long": return typeof(long);
                    case "float": return typeof(float);
                    case "double": return typeof(double);
                    case "decimal": return typeof(decimal);
                    case "bool": return typeof(bool);
                    case "DateTime": return typeof(DateTime);
                    case "byte[]": return typeof(byte[]);
                    case "TimeSpan": return typeof(TimeSpan);
                    case "DateTimeOffset": return typeof(DateTimeOffset);
                    case "byte": return typeof(byte);
                    case "short": return typeof(short);
                    default: return typeof(object);
                }
            }
        }

    }
}
