// See https://aka.ms/new-console-template for more information

using Dapper.Sharding;
using System.Reflection;

var config = new DataBaseConfig { Password = "123" };
var client = ShardingFactory.CreateClient(DataBaseType.MySql, config);
client.AutoCompareTableColumn = true;
client.AutoCompareTableColumnDelete = true;
//TypeHandlerJsonNet.Add(Assembly.GetExecutingAssembly());
TypeHandlerSystemTextJson.Add(Assembly.GetExecutingAssembly());

var db = client.GetDatabase("test");
var table = db.GetTable<People>("pss22122");

var people = new People
{
    Name = "哈哈",
    MyA = new A { Name = "sss" },
    MyB = new B { Age = 2 }
};

table.Insert(people);

Console.WriteLine(table.GetById(1).MyA.Name);

Console.WriteLine("Hello, World!");

[Table("Id")]
public class People
{
    public int Id { get; set; }

    public string Name { get; set; }

    [Column(columnType:"jsons")]
    public A MyA { get; set; }

    [Column(columnType: "jsons")]
    public B MyB { get; set; }
}

public class A
{
    public string Name { get; set; }
}

public class B
{
    public int Age { get; set; }
}