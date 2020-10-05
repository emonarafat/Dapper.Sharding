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
        public void TruncateTable()
        {
            Factory.Db.TruncateTable("P2");
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
            var data = Factory.Db.GetTableColumnList("People");
            Assert.Pass(JsonConvert.SerializeObject(data));
        }

        [Test]
        public void ExistsTable()
        {
            Console.WriteLine(Factory.Db.ExistsTable("People"));
            Console.WriteLine(Factory.Db.ExistsTable("People2222"));
        }

        [Test]
        public void ShowTableScript()
        {
            Console.WriteLine(Factory.Db.GetTableScript<People>("People"));
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
        public void GeneratorClassFile()
        {
            Factory.Db.GeneratorClassFile("D:\\");
        }

    }
}
