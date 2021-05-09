using FastMember;
using System;

namespace Dapper.Sharding
{
    public class ShardingUtils
    {
        public static int Mod(object id, int count)
        {
            string key;
            if (id.GetType() == typeof(string))
            {
                key = (string)id;
            }
            else
            {
                key = id.ToString();
            }
            return HashBloomFilter.BKDRHash(key) % count;
        }

        public static int Mod<T>(T model, string keyName, Type keyType, int count)
        {
            var accessor = TypeAccessor.Create(typeof(T));
            var id = accessor[model, keyName];
            string key;
            if (keyType == typeof(string))
            {
                key = (string)id;
            }
            else
            {
                key = id.ToString();
            }
            return HashBloomFilter.BKDRHash(key) % count;
        }

    }
}
