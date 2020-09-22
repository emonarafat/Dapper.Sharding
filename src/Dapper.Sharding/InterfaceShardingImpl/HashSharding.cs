using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dapper.Sharding
{
    internal class HashSharding<T> : ISharding<T>
    {
        public override bool Delete(object id)
        {
            return GetTableById(id).Delete(id);
        }

        public override int DeleteByIds(object ids)
        {
            throw new NotImplementedException();
        }

        public override bool Exists(object id)
        {
            return GetTableById(id).Exists(id);
        }

        public override T GetById(object id, string returnFields = null)
        {
            return GetTableById(id).GetById(id, returnFields);
        }

        public override IEnumerable<T> GetByIds(object ids, string returnFields = null)
        {
            throw new NotImplementedException();
        }

        public override bool Insert(T model)
        {
            return GetTableByModelAndInitId(model).Insert(model);
        }

        public override bool InsertIfExistsUpdate(T model, string fields = null)
        {
            return GetTableByModel(model).InsertIfExistsUpdate(model, fields);
        }

        public override bool InsertIfNoExists(T model)
        {
            return GetTableByModel(model).InsertIdentityIfNoExists(model);
        }

        public override int InsertMany(IEnumerable<T> modelList)
        {
            throw new NotImplementedException();
        }

        public override bool Update(T model)
        {
            return GetTableByModel(model).Update(model);
        }

        public override bool UpdateExclude(T model, string fields)
        {
            return GetTableByModel(model).UpdateExclude(model, fields);
        }

        public override int UpdateExcludeMany(IEnumerable<T> modelList, string fields)
        {
            throw new NotImplementedException();
        }

        public override bool UpdateInclude(T model, string fields)
        {
            return GetTableByModel(model).UpdateInclude(model, fields);
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
