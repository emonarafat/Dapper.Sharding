using Newtonsoft.Json.Converters;

namespace Dapper.Sharding
{
    public class JsonNetDataTimeDayConverter : IsoDateTimeConverter
    {
        public JsonNetDataTimeDayConverter()
        {
            // 默认日期时间格式
            DateTimeFormat = "yyyy-MM-dd";
        }

        public JsonNetDataTimeDayConverter(string format)
        {
            DateTimeFormat = format;
        }
    }
}
