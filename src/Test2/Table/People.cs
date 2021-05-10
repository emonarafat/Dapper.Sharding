using Dapper.Sharding;
using System;

namespace Test2
{
    //[Index("Name","Name",IndexType.Normal)]
    //[Index("Age","Age",IndexType.Unique)]
    //[Index("NameAndAge", "Name,Age", IndexType.Unique)]
    [Table("Id", true, "人类表", "Log")]
    public class People
    {
        [Column(11, "主键id")]
        public long Id { get; set; }

        public string Name { get; set; }

        public DateTime Time { get; set; }

        [Column(-1)]
        public DateTime Time1 { get; set; }

        [Column(-2)]
        public DateTime Time2 { get; set; }

        public float Float { get; set; }

        public double Double { get; set; }

        public long Age { get; set; }

        [Column(18.2, "金钱一")]
        public decimal Money { get; set; }

        public int IsAdmin { get; set; }

        public DateTime AddTime { get; set; }

        public short ShortField { get; set; }

        public byte ByteField { get; set; }

        [Ignore]
        public string NoDataBaseColumn { get; set; }
    }
}
