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

        public static int ModAndInitId<T>(T model, string keyName, Type keyType, int count)
        {

            var accessor = TypeAccessor.Create(typeof(T));
            var id = accessor[model, keyName];
            string key;
            if (keyType == typeof(string))
            {
                key = (string)id;
                if (string.IsNullOrEmpty(key))
                {
                    key = ObjectId.GenerateNewIdAsString();
                    accessor[model, keyName] = key;
                }
            }
            else if (keyType == typeof(long))
            {
                if ((long)id == 0)
                {
                    var newId = SnowflakeId.GenerateNewId();
                    key = newId.ToString();
                    accessor[model, keyName] = newId;
                }
                else
                {
                    key = id.ToString();
                }
            }
            else
            {
                key = id.ToString();
            }
            return HashBloomFilter.BKDRHash(key) % count;
        }

    }
}
