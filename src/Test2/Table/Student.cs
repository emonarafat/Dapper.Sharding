using Dapper.Sharding;

namespace Test2
{
    [Table("Id", false, "学生表", "Memory")]
    public class Student
    {
        [Column(24, "主键id")]
        public string Id { get; set; }

        [Column(0, "名字")]
        public string Name { get; set; }

        [Column(0, "年龄")]
        public long Age { get; set; }

    }
}
