using Newtonsoft.Json;
using NUnit.Framework;
using System;
using Test.Com;

namespace Test
{
    class TestDataBase
    {
        [Test]
        public void CreateTable()
        {
            Factory.Db.GetTable<People>("People");
            Factory.Db.GetTable<Teacher>("Teacher");
            Factory.Db.GetTable<Student>("Student");
            Factory.Db.GetTable<People>("P2");
        }

        [Test]
        public void DropTable()
        {
            Factory.Db.DropTable("P2");
        }

        [Test]
        public void ShowTableList()
        {
            var data = Factory.Db.ShowTableList();
            Assert.Pass(JsonConvert.SerializeObject(data));
        }

        [Test]
        public void ExistsTable()
        {
            Console.WriteLine(Factory.Db.ExistsTable("People"));
            Console.WriteLine(Factory.Db.ExistsTable("People22222"));
        }

        [Test]
        public void ShowTableScript()
        {
            Console.WriteLine(Factory.Db.ShowTableScript<People>("People"));
            Console.WriteLine("\r\n");
            Console.WriteLine("\r\n");
            Console.WriteLine(Factory.Db.ShowTableScript<Student>("sss"));
        }

        [Test]
        public void ShowTableStatus()
        {
            object data = Factory.Db.ShowTableStatus("People");
            Console.WriteLine(JsonConvert.SerializeObject(data));
        }

        [Test]
        public void ShowTableStatusList()
        {
            Console.WriteLine(JsonConvert.SerializeObject(Factory.Db.ShowTableStatusList()));
        }

        [Test]
        public void GetTableEntityFromDatabase()
        {
            Console.WriteLine(JsonConvert.SerializeObject(Factory.Db.GetTableEntityFromDatabase("People")));
        }

        [Test]
        public void GetTableEnityListFromDatabase()
        {
            Console.WriteLine(JsonConvert.SerializeObject(Factory.Db.GetTableEnityListFromDatabase()));
        }

        [Test]
        public void GeneratorClassFile()
        {
            Factory.Db.GeneratorClassFile("D:\\");
        }

    }
}
