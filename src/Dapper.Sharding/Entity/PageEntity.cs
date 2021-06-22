using System.Collections.Generic;

namespace Dapper.Sharding
{
    public class PageEntity<T>
    {
        public long Count { get; set; }

        public IEnumerable<T> Data { get; set; }
    }
}
