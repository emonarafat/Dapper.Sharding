using System.Collections.Generic;

namespace Dapper.Sharding
{
    public class LockManager
    {
        private object _lock = new object();

        private Dictionary<string, object> dict = new Dictionary<string, object>();

        public object GetObject(string name)
        {
            if (!dict.ContainsKey(name))
            {
                lock (_lock)
                {
                    if (!dict.ContainsKey(name))
                    {
                        dict.Add(name, new object());
                    }
                }
            }
            return dict[name];
        }
    }
}
