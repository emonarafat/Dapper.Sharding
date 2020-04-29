using System.Collections.Generic;

namespace Dapper.Sharding
{
    public class LockManager
    {
        private object _lock = new object();

        private Dictionary<object, object> dict = new Dictionary<object, object>();

        public object GetObject(object name)
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
