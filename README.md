#sharding for mysql、postgresql、sqlserver、sqlite、clickhouse、oracle

```csharp
var config = new DataBaseConfig { Server = "127.0.0.1", UserId = "root", Password = "123", Port = 3306 };

//client must be singleton mode(必须是单例模式)
static IClient client = ShardingFactory.CreateClient(DataBaseType.MySql, config); 
//client.AutoCreateDatabase = true;
//client.AutoCreateTable = true;
//client.AutoCompareTableColumn = false;
//client.AutoCompareTableColumnDelete = false;

var db = client.GetDatabase("test");
//var db2 = client.GetDatabase("test2"); //this will create test2 database(自由分库)

var table = db.GetTable<Student>("student"); //自由分表
table.Insert(new Student { Id = ShardingFactory.NextObjectId(), Name = "lina" });

var table2 = db.GetTable<Student>("student2");
table2.Insert(new Student { Id = ShardingFactory.NextObjectId(), Name = "lina2" });

var table3 = db.GetTable<Student>("student3");
table3.Insert(new Student { Id = ShardingFactory.NextObjectId(), Name = "lina3" });

//sharding query on all table(分片查询)
var query = new ShardingQuery(table, table2, table3); 
var total = await query.CountAsync();
or
var data = await query.QueryAsync("SELECT * FROM $table"); //$table is each table name

//Transaction(分布式事务)
var tran = new DistributedTransaction();
try
{
    table.Insert(new Student { Id = ShardingFactory.NextObjectId(), Name = "lina" }, tran);
    table.Delete("1", tran);
    table2.Delete("2", tran);
    table3.Delete("3", tran);
    tran.Commit();
}
catch
{
    tran.Rollback();    
}

//Transaction CAP
//https://gitee.com/znyet/dapper.sharding.cap

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
```

```csharp
//custom column type
[Column(columnType: "text")]
[Column(columnType: "geometry")]
```

```csharp
//client must singleton mode(必须是单例模式)

/*===mysql need MySqlConnector===*/
public static IClient Client = ShardingFactory.CreateClient(DataBaseType.MySql, new DataBaseConfig { Server = "127.0.0.1", UserId = "root", Password = "123", Port = 3306 })

/*===sqlite need System.Data.SQLite.Core===*/
//public static IClient Client = ShardingFactory.CreateClient(DataBaseType.Sqlite, new DataBaseConfig { Server = "D:\\DatabaseFile" })

/*===sqlserver need System.Data.SqlClient ===*/
//public static IClient Client = ShardingFactory.CreateClient(DataBaseType.SqlServer2008, new DataBaseConfig { Server = ".\\express", UserId = "sa", Password = "123456", Database_Path = "D:\\DatabaseFile" })

/*===clickhouse need ClickHouse.Ado.Dapper ===*/
//public static IClient ClientHouse = ShardingFactory.CreateClient(DataBaseType.ClickHouse, new DataBaseConfig { Server = "192.168.0.200" });

/*===postgresql need Npgsql===*/
//public static IClient Client = ShardingFactory.CreateClient(DataBaseType.Postgresql, new DataBaseConfig { Server = "127.0.0.1", UserId = "postgres", Password = "123" })

/*===oracle need Oracle.ManagedDataAccess.Core===*/
static DataBaseConfig oracleConfig = new DataBaseConfig
{
    Server = "127.0.0.1",
    UserId = "test",
    Password = "123",
    Oracle_ServiceName = "xe",
    Oracle_SysUserId = "sys",
    Oracle_SysPassword = "123",
    Database_Path = "D:\\DatabaseFile",
    Database_Size_Mb = 1,
    Database_SizeGrowth_Mb = 1
};
//public static IClient Client = ShardingFactory.CreateClient(DataBaseType.Oracle, oracleConfig)
```

```csharp
//json or json string field
namespace ConsoleApp1
{
    [Table("Id", true, "人类")]
    public class School
    {
        public int Id { get; set; }

        public string Name { get; set; }

        //[Column(columnType: "varchar(8000)")]
        [Column(columnType: "jsonb")]
        public Student Stu { get; set; }  //json or json string
    }

    public class Student
    {
        public string Name { get; set; }

        public int Age { get; set; }
    }
}

var config = new DataBaseConfig { Password = "123" };
var client = ShardingFactory.CreateClient(DataBaseType.Postgresql, config);
client.AutoCompareTableColumn = true;

//TypeHandlerJsonNet.Add<Student>(); //json.net
TypeHandlerSystemTextJson.Add<Student>(); //System.Text.Json add dapper typehandler

var db = client.GetDatabase("test");
var table = db.GetTable<School>("school");

var school = new School
{
    Name = "test",
    Stu = new Student
    {
        Name = "lihua",
        Age = 18
    }
};
table.Insert(school);
//table.InsertMany(list);
var model = table.GetById(1);
Console.WriteLine(model.Stu.Name);

//System.Text.Json
TypeHandlerSystemTextJson.Add<Student>();
TypeHandlerSystemTextJson.Add<List<Student>>();
TypeHandlerSystemTextJson.Add<JsonObject>();
TypeHandlerSystemTextJson.Add<JsonArray>();

//Json.Net
TypeHandlerJsonNet.Add<Student>();
TypeHandlerJsonNet.Add<List<Student>>();
TypeHandlerJsonNet.Add<JObject>();
TypeHandlerJsonNet.Add<JArray>();
```

```csharp
GeneratorClassFile(can create class entity file from database) //代码生成器

db.GeneratorTableFile("D:\\Class"); //生成表实体类

db.GeneratorDbContextFile("D:\\Class"); //生成请求上下文文件
```

```csharp
//Npgsql GeoJson
NpgsqlConnection.GlobalTypeMapper.UseGeoJson();
NpgsqlGeoJsonFactory.UseGeoJson();

//Npgsql NetTopologySuite
NpgsqlConnection.GlobalTypeMapper.UseNetTopologySuite();
NpgsqlGeoFactory.UseGeo();
```