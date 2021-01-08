using System.Collections.Generic;

namespace Dapper.Sharding
{
    public class LockManager
    {
        private object _lock = new object();

        private Dictionary<string, object> dict = new Dictionary<string, object>();

        public object GetObject(string name)
        {
            var exists = dict.TryGetValue(name, out var val);
            if (!exists)
            {
                lock (_lock)
                {
                    if (!dict.ContainsKey(name))
                    {
                        val = new object();
                        dict.Add(name, val);
                    }
                }
            }
            return val;
        }
    }
}
