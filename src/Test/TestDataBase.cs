using Newtonsoft.Json;
using NUnit.Framework;
using System;
using Test.Com;

namespace Test
{
    class TestDataBase
    {
        [Test]
        public void GetTable()
        {
            Factory.Db.GetTable<People>("people");
            Factory.Db.GetTable<Teacher>("teacher");
            Factory.Db.GetTable<Student>("student");
            Factory.Db.GetTable<People>("p2");
        }

        [Test]
        public void DropTable()
        {
            Factory.Db.DropTable("p2");
        }

        [Test]
        public void TruncateTable()
        {
            Factory.Db.TruncateTable("p2");
        }

        [Test]
        public void GetTableList()
        {
            var data = Factory.Db.GetTableList();
            Assert.Pass(JsonConvert.SerializeObject(data));
        }

        [Test]
        public void GetTableColumnList()
        {
            var data = Factory.Db.GetTableColumnList("people");
            Assert.Pass(JsonConvert.SerializeObject(data));
        }

        [Test]
        public void ExistsTable()
        {
            Console.WriteLine(Factory.Db.ExistsTable("people"));
            Console.WriteLine(Factory.Db.ExistsTable("people2222"));
        }

        [Test]
        public void ShowTableScript()
        {
            Console.WriteLine(Factory.Db.GetTableScript<People>("people"));
            Console.WriteLine("\r\n");
            Console.WriteLine("\r\n");
            Console.WriteLine(Factory.Db.GetTableScript<Student>("sss"));
        }

        [Test]
        public void GetTableEntityFromDatabase()
        {
            Console.WriteLine(JsonConvert.SerializeObject(Factory.Db.GetTableEntityFromDatabase("people")));
        }

        [Test]
        public void GeneratorTableFile()
        {
            Factory.Db.GeneratorTableFile("D:\\Class");
        }

        [Test]
        public void GeneratorDbContextFile()
        {
            Factory.Db.GeneratorDbContextFile("D:\\Class");
        }

    }
}
