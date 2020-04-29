using System.Collections.Generic;

namespace Dapper.Sharding
{
    public class TableEntity
    {
        public string Name { get; set; }

        public string Comment { get; set; }

        public string PrimaryKey { get; set; }

        public bool IsIdentity { get; set; }

        public List<ColumnEntity> ColumnList { get; set; }

        public List<IndexEntity> IndexList { get; set; }

    }
}
