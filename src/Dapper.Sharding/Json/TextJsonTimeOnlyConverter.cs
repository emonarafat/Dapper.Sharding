#if CORE6
using System;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Dapper.Sharding
{
    public class TextJsonTimeOnlyConverter : JsonConverter<TimeOnly>
    {
        public override TimeOnly Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            TimeOnly.TryParse(reader.GetString(), out var val);
            return val;
        }

        public override void Write(Utf8JsonWriter writer, TimeOnly value, JsonSerializerOptions options)
        {
            writer.WriteStringValue(value.ToString("HH:mm:ss"));
        }
    }
}
#endif