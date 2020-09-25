using System;
using System.Collections.Generic;
using System.Linq;

namespace Dapper.Sharding
{
    internal class RangeSharding<T> : ISharding<T>
    {
        private Dictionary<long, ITable<T>> _dict;
        private IEnumerable<long> _rangeList;

        public RangeSharding(Dictionary<long, ITable<T>> dict) : base(dict.Values.ToArray())
        {
            _dict = dict;
            _rangeList = dict.Keys.AsEnumerable().OrderBy(b => b).AsEnumerable();
        }

        public override ITable<T> GetTableById(string id)
        {
            var range = _rangeList.First(f => f <= Convert.ToInt64(id));
            return _dict[range];
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

        public override ITable<T> GetTableByModelAndInitId(T model)
        {
            var accessor = TypeAccessor.Create(typeof(T));
            var id = (long)accessor[model, KeyName];
            if (id == 0)
            {
                id = SnowflakeId.GenerateNewId();
                accessor[model, KeyName] = id;
            }
            var range = _rangeList.First(f => f <= id);
            return _dict[range];
        }

        public override bool Delete(object id)
        {
            throw new NotImplementedException();
        }

        public override int DeleteByIds(object ids)
        {
            throw new NotImplementedException();
        }

        public override bool Exists(object id)
        {
            throw new NotImplementedException();
        }

        public override T GetById(object id, string returnFields = null)
        {
            throw new NotImplementedException();
        }

        public override IEnumerable<T> GetByIds(object ids, string returnFields = null)
        {
            throw new NotImplementedException();
        }

        public override bool Insert(T model)
        {
            throw new NotImplementedException();
        }

        public override bool InsertIfExistsUpdate(T model, string fields = null)
        {
            throw new NotImplementedException();
        }

        public override bool InsertIfNoExists(T model)
        {
            throw new NotImplementedException();
        }

        public override int InsertMany(IEnumerable<T> modelList)
        {
            throw new NotImplementedException();
        }

        public override bool Update(T model)
        {
            throw new NotImplementedException();
        }

        public override bool UpdateExclude(T model, string fields)
        {
            throw new NotImplementedException();
        }

        public override int UpdateExcludeMany(IEnumerable<T> modelList, string fields)
        {
            throw new NotImplementedException();
        }

        public override bool UpdateInclude(T model, string fields)
        {
            throw new NotImplementedException();
        }

        public override int UpdateIncludeMany(IEnumerable<T> modelList, string fields)
        {
            throw new NotImplementedException();
        }

        public override int UpdateMany(IEnumerable<T> modelList)
        {
            throw new NotImplementedException();
        }
    }
}
