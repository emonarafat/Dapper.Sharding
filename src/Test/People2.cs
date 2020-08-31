using Dapper.Sharding;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Test
{
    [Table("Id", false, "人类表")]
    class People2
    {
        [Column(24, "主键id")]
        public string Id { get; set; }

        [Column(50, "名字")]
        public string Name { get; set; }

        [Column(comment: "年龄")]
        public long Age { get; set; }

    }
}
