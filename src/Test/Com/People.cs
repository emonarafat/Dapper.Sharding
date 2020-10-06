using Dapper.Sharding;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Test
{
    [Index("Name","Name",IndexType.Normal)]
    [Index("Age","Age",IndexType.Unique)]
    [Index("NameAndAge", "Name,Age", IndexType.Unique)]
    [Table("Id", false, "人类表")]
    public class People
    {
        [Column(11, "主键id")]
        public int Id { get; set; }

        [Column(50, "名字")]
        public string Name { get; set; }

        [Column(20, "年龄")]
        public long Age { get; set; }

        [Column(-1, "长文章")]
        public string Text { get; set; }

        [Column(-2, "超级长文章")]
        public string LongText { get; set; }

        [Column(18.2, "金钱一")]
        public decimal Money { get; set; }

        public float Money2 { get; set; }

        public double Money3 { get; set; }

        public bool IsAdmin { get; set; }

        public DateTime AddTime { get; set; }

        public short ShortField { get; set; }

        public byte ByteField { get; set; }

        [Ignore]
        public string NoDataBaseColumn { get; set; }
    }
}
