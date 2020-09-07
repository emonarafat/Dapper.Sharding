using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dapper.Sharding
{
    public class SqlFieldEntity
    {
        public SqlFieldEntity(string primaryKey, string isIdentity, string allFields, string allFieldsAt, string allFieldsAtEq, string allFieldsExceptKey, string allFieldsAtExceptKey, string allFieldsAtEqExceptKey)
        {

        }

        public string PrimaryKey { get; }

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
