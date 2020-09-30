using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Dapper.Sharding
{
    internal class RangeSharding<T> : ISharding<T> where T : class
    {
        private Dictionary<long, ITable<T>> _dict;
        private IEnumerable<long> _rangeList;

        public RangeSharding(Dictionary<long, ITable<T>> dict) : base(dict.Values.ToArray())
        {
            _dict = dict;
            _rangeList = dict.Keys.AsEnumerable().OrderBy(b => b).AsEnumerable();
        }

        public override ITable<T> GetTableById(object id)
        {
            var range = _rangeList.First(f => f <= (long)id);
            return _dict[range];
        }

        public override ITable<T> GetTableByModel(T model)
        {
            var accessor = TypeAccessor.Create(typeof(T));
            var id = (long)accessor[model, KeyName];
            var range = _rangeList.First(f => f <= id);
            return _dict[range];
        }
    }
}
