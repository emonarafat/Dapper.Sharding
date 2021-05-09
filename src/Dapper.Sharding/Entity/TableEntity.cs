using System;
using System.Collections.Generic;

namespace Dapper.Sharding
{
    public class TableEntity
    {
        public string PrimaryKey { get; set; }

        public Type PrimaryKeyType { get; set; }

        public bool IsIdentity { get; set; }

        public string Comment { get; set; }

        public string Engine { get; set; }

        public List<ColumnEntity> ColumnList { get; set; }

        public Dictionary<string, double> OtherColumnDict { get; set; }

        public List<IndexEntity> IndexList { get; set; }

        public List<string> IgnoreColumnList { get; set; }
    }
}
