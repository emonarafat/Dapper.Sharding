using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Dapper.Sharding;

namespace Test
{
    [Table("Id", true, "嘿嘿")]
    public class People
    {
        public int Id { get; set; }

        [Column(22, "才尼玛")]
        public string Name { get; set; }

        //[Column(18.5,"钱多多")]
        //public decimal MoneyDuoduo { get; set; }
    }

    [Index("AddTime", "AddTime", IndexType.Unique)]
    [Index("age", "age,height", IndexType.Normal)]
    [Table("Id", true, "人类")]
    public class School
    {
        public int Id { get; set; }

        public int Age { get; set; }

        public long Height { get; set; }

        public decimal Money { get; set; }

        public double Dbbbb { get; set; }

        public float fff { get; set; }

        public DateTime AddTime { get; set; }

        public bool IsOk { get; set; }

        [Column(comment: "测试guid")]
        public Guid uuid { get; set; }

        [Column(100, "收货地址")]
        public string Address { get; set; }

        [Column(5.3, "小钱")]
        public decimal LittleMoney { get; set; }


    }

}
