using Newtonsoft.Json.Converters;

namespace Dapper.Sharding
{
    public class JsonNetDataTimeConverter : IsoDateTimeConverter
    {
        public JsonNetDataTimeConverter()
        {
            // 默认日期时间格式
            DateTimeFormat = "yyyy-MM-dd HH:mm:ss";
        }

        public JsonNetDataTimeConverter(string format)
        {
            DateTimeFormat = format;
        }
    }
}


/// <summary>
/// 使用方法一(属性标注)
///// </summary>
//[JsonConverter(typeof(JsonNetDataTimeConverter))]
//public DateTime PayTime { get; set; }
//
//或者
//[JsonConverter(typeof(JsonNetDataTimeConverter),"yyyy-MM-dd")]
//public DateTime PayTime { get; set; }


/// <summary>
/// 使用方法二(局部)
///// </summary>
//IsoDateTimeConverter timeFormat = new IsoDateTimeConverter();
//timeFormat.DateTimeFormat = "yyyy-MM-dd HH:mm:ss";
//JsonConvert.SerializeObject(stu, timeFormat);


/// <summary>
/// 使用方法三(全局)
///// </summary>
//JsonSerializerSettings setting = new JsonSerializerSettings();
//JsonConvert.DefaultSettings = new Func<JsonSerializerSettings>(() =>
//{
//    //日期类型默认格式化处理
//    setting.DateFormatHandling = Newtonsoft.Json.DateFormatHandling.MicrosoftDateFormat;
//    setting.DateFormatString = "yyyy-MM-dd HH:mm:ss";

//    //空值处理
//    setting.NullValueHandling = NullValueHandling.Ignore;

//    return setting;
//});