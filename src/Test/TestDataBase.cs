using Newtonsoft.Json;
using NUnit.Framework;
using System;

namespace Test
{
    [TestFixture]
    public class TestDataBase
    {

        [Test]
        public void GetConn()
        {
            using (var conn = Factory.GetDatabase().GetConn())
            {
                Console.WriteLine("获取conn成功");
            }
        }

        [Test]
        public void SetCharset()
        {
            Factory.GetDatabase().SetCharset("utf8");
            //db.AlertCharset("utf8mb4");
        }

        [Test]
        public void DropTable()
        {
            Factory.GetDatabase().DropTable(Factory.Table);
        }

        [Test]
        public void GetTables()
        {
            var data = Factory.GetDatabase().GetTables();
            Console.WriteLine(JsonConvert.SerializeObject(data));
        }

        [Test]
        public void GetTableStatus()
        {
            var data = Factory.GetDatabase().GetTableStatus();
            Console.WriteLine(JsonConvert.SerializeObject(data));
        }

        [Test]
        public void Using()
        {
            Factory.GetDatabase().Using(conn =>
            {
                Console.WriteLine("引用自动释放无返回值");
            });
        }

        [Test]
        public void Using2()
        {
            string data = Factory.GetDatabase().Using(conn =>
            {
                return "引用自动释放并返回值";
            });

            Console.WriteLine(data);
        }

        [Test]
        public void UsingTran1()
        {
            Factory.GetDatabase().UsingTran((conn, tran) =>
            {
                try
                {
                    Console.WriteLine("Tran无返回值33333");
                    tran.Commit();
                }
                catch
                {
                    tran.Rollback();
                }

            });
        }

        [Test]
        public void UsingTran2()
        {
            var data = Factory.GetDatabase().UsingTran((conn, tran) =>
            {
                try
                {
                    tran.Commit();
                    return "Tran有返回值33333";

                }
                catch
                {
                    tran.Rollback();
                    return null;
                }

            });

            Console.WriteLine(data);
        }

        [Test]
        public void GetTableEntitys()
        {
            var data = Factory.GetDatabase().GetTableEnitys();
            Console.WriteLine(JsonConvert.SerializeObject(data));
        }

        [Test]
        public void CreateTable()
        {
            Factory.GetDatabase().CreateTable<School>("sss");
        }

        [Test]
        public void CreateTableScript()
        {
            var script = Factory.GetDatabase().CreateTableScript<School>("dd");
            Console.WriteLine(script);
        }

        [Test]
        public void GetTable()
        {
            var db = Factory.GetDatabase();

            db.Using(conn =>
            {
                var table = db.GetTable<School>("SchoolQQQ11111", conn);
                var table2 = db.GetTable<People>("peopleQQQ1111", conn);
            });
        }

    }
}
