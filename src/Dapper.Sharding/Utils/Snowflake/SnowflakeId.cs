namespace Dapper.Sharding
{
    public class SnowflakeId
    {
        internal static IdWorker worker = new IdWorker(0, 0);

        public static long GenerateNewId()
        {
            return worker.NextId();
        }
    }
}
