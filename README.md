#sharding for mysql、postgresql、sqlserver、sqlite、clickhouse、oracle

```csharp
var config = new DataBaseConfig { Server = "127.0.0.1", UserId = "root", Password = "123", Port = 3306 };

//client must be singleton mode(必须是单例模式)
static IClient client = ShardingFactory.CreateClient(DataBaseType.MySql, config); 
//client.AutoCreateDatabase = true;
//client.AutoCreateTable = true;
//client.AutoCompareTableColumn = false;

var db = client.GetDatabase("test");
//var db2 = client.GetDatabase("test2"); //this will create test2 database

var table = db.GetTable<Student>("student");
table.Insert(new Student { Id = ShardingFactory.NextObjectId(), Name = "lina" });

var table2 = db.GetTable<Student>("student2");
table2.Insert(new Student { Id = ShardingFactory.NextObjectId(), Name = "lina2" });

var table3 = db.GetTable<Student>("student3");
table3.Insert(new Student { Id = ShardingFactory.NextObjectId(), Name = "lina3" });


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
GeneratorClassFile(can create class entity file from database)
db.GeneratorClassFile("D:\Class")
```