```csharp
var config = new DataBaseConfig { Server = "127.0.0.1", UserId = "root", Password = "123", Port = 3306 };
//client must be singleton mode(必须是单例模式)
static IClient client = ShardingFactory.CreateClient(DataBaseType.MySql, config); 
//client.AutoCreateDatabase = true;
//client.AutoCreateTable = true;
//client.AutoCompareTableColumn = false;
var table = client.GetDatabase("test").GetTable<Student>("student");
//var table2 = client.GetDatabase("test").GetTable<Student>("student2");
//var table3 = client.GetDatabase("test").GetTable<Student>("student3");

table.Insert(new Student { Id = ShardingFactory.NextObjectId(), Name = "lina" });

namespace ConsoleApp
{
    [Table("Id", false, "学生表")]
    public class Student
    {
        [Column(24, "主键id")]
        public string Id { get; set; }

        [Column(50, "名字")]
        public string Name { get; set; }

        [Column(20, "年龄")]
        public long Age { get; set; }

        [Ignore]
        public string NoDatabaseColumn { get; set; }
    }
}