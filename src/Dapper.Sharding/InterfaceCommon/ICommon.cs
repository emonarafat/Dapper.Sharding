using System.Collections.Generic;

namespace Dapper.Sharding
{
    public interface ICommon<T> where T : class
    {
        T GetById(object id, string returnFields = null);

        IEnumerable<T> GetByIds(object ids, string returnFields = null);

        IEnumerable<T> GetByIdsWithField(object ids, string field, string returnFields = null);

        IEnumerable<T> GetByWhere(string where, object param = null, string returnFields = null, string orderby = null, int limit = 0);

        SqlFieldEntity SqlField { get; }
    }
}
