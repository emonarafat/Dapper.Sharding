using System.Collections.Generic;

namespace Dapper.Sharding
{
    public class LockManager
    {
        private readonly object _lock = new object();

        private readonly Dictionary<string, object> dict = new Dictionary<string, object>();

        public object GetObject(string name)
        {
            var exists = dict.TryGetValue(name, out var val);
            if (!exists)
            {
                lock (_lock)
                {
                    if (!dict.ContainsKey(name))
                    {
                        dict.Add(name, new object());
                    }
                }
                val = dict[name];
            }  
            return val;
        }
    }
}
