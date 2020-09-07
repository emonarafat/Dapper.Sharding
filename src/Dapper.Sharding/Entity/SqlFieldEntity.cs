using System;
using System.Linq;

namespace Dapper.Sharding
{
    public class SqlFieldEntity
    {
        public SqlFieldEntity(TableEntity entity, string leftChar, string rightChart, string symbol)
        {
            this.PrimaryKey = entity.PrimaryKey;
            this.IsIdentity = entity.IsIdentity;
            this.PrimaryKeyType = entity.PrimaryKeyType;

            var allFieldList = entity.ColumnList.Select(s => s.Name);
            var allFieldExceptKeyList = allFieldList.Where(w => w.ToLower() != entity.PrimaryKey.ToLower());

            this.AllFields = CommonUtil.GetFieldsStr(allFieldList, leftChar, rightChart);
            this.AllFieldsAt = CommonUtil.GetFieldsAtStr(allFieldList, symbol);
            this.AllFieldsAtEq = CommonUtil.GetFieldsAtEqStr(allFieldList, leftChar, rightChart, symbol);

            this.AllFieldsExceptKey = CommonUtil.GetFieldsStr(allFieldExceptKeyList, leftChar, rightChart);
            this.AllFieldsAtExceptKey = CommonUtil.GetFieldsAtStr(allFieldExceptKeyList, symbol);
            this.AllFieldsAtEqExceptKey = CommonUtil.GetFieldsAtEqStr(allFieldExceptKeyList, leftChar, rightChart, symbol);
        }

        public string PrimaryKey { get; }

        public Type PrimaryKeyType { get; }

        public bool IsIdentity { get; }

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
