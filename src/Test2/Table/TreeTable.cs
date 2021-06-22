using Dapper.Sharding;
using System;

namespace Test2
{
    //SETTINGS index_granularity = 8192 
    [Table("id", false, "合并树", "MergeTree() PARTITION BY toYYYYMMDD(date) ORDER BY id ")]
    public class TreeTable
    {
        public string id { get; set; }

        [Column(-1)]
        public DateTime date { get; set; }

        public DateTime time { get; set; }

        public int age { get; set; }
    }
}

//OPTIMIZE TABLE [db.]name [ON CLUSTER cluster] [PARTITION partition | PARTITION ID 'partition_id'] [FINAL] [DEDUPLICATE]
//OPTIMIZE TABLE t PARTITION 201704