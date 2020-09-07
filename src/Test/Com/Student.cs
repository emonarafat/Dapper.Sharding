using Dapper.Sharding;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Test
{
    [Table("Id", false, "学生表")]
    class Student
    {
        [Column("主键id")]
        public string Id { get; set; }

        [Column("名字", 50)]
        public string Name { get; set; }

        [Column("年龄")]
        public long Age { get; set; }

    }
}
