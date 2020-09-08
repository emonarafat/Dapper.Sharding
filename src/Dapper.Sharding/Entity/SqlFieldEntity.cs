using System;
using System.Collections.Generic;
using System.Linq;

namespace Dapper.Sharding
{
    public class SqlFieldEntity
    {
        public SqlFieldEntity(TableEntity entity, string leftChar, string rightChart, string symbol)
        {
            PrimaryKey = entity.PrimaryKey;
            IsIdentity = entity.IsIdentity;
            PrimaryKeyType = entity.PrimaryKeyType;

            AllFieldList = entity.ColumnList.Select(s => s.Name);
            AllFieldExceptKeyList = AllFieldList.Where(w => w.ToLower() != entity.PrimaryKey.ToLower());

            AllFields = CommonUtil.GetFieldsStr(AllFieldList, leftChar, rightChart);
            AllFieldsAt = CommonUtil.GetFieldsAtStr(AllFieldList, symbol);
            AllFieldsAtEq = CommonUtil.GetFieldsAtEqStr(AllFieldList, leftChar, rightChart, symbol);

            AllFieldsExceptKey = CommonUtil.GetFieldsStr(AllFieldExceptKeyList, leftChar, rightChart);
            AllFieldsAtExceptKey = CommonUtil.GetFieldsAtStr(AllFieldExceptKeyList, symbol);
            AllFieldsAtEqExceptKey = CommonUtil.GetFieldsAtEqStr(AllFieldExceptKeyList, leftChar, rightChart, symbol);
        }

        public string PrimaryKey { get; }

        public Type PrimaryKeyType { get; }

        public bool IsIdentity { get; }

        public IEnumerable<string> AllFieldList { get; }

        public IEnumerable<string> AllFieldExceptKeyList { get; }

        //保留主键
        public string AllFields { get; }//所有列逗号分隔[name],[sex]

        public string AllFieldsAt { get; } //@name,@sex

        public string AllFieldsAtEq { get; }//[name]=@name,[sex]=@sex

        //去除主键
        public string AllFieldsExceptKey { get; }//所有列逗号分隔[name],[sex]

        public string AllFieldsAtExceptKey { get; } //@name,@sex

        public string AllFieldsAtEqExceptKey { get; }//[name]=@name,[sex]=@sex

    }
}
