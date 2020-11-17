namespace Dapper.Sharding
{
    internal class SnowflakeId
    {
        internal static IdWorker worker = new IdWorker(0, 0);

        public static long GenerateNewId()
        {
            return worker.NextId();
        }
    }
}
