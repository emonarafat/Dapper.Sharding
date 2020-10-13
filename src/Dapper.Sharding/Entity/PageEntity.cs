using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dapper.Sharding
{
    public class PageEntity<T>
    {
        public long Count { get; set; }

        public IEnumerable<T> Data { get; set; }
    }
}
