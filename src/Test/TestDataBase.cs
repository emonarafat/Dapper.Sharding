using Dapper;
using Dapper.Sharding;
using Newtonsoft.Json;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Test
{
    class TestDataBase
    {
        [Test]
        public void CreateTable()
        {
            Factory.Db.CreateTable<People>("People");
            Factory.Db.CreateTable<People>("People2");
            Factory.Db.CreateTable<People>("P");

            Factory.Db.CreateTable<Student>("Student");
            Factory.Db.CreateTable<Student>("Student2");
            Factory.Db.CreateTable<Student>("S");
        }

        [Test]
        public void DropTable()
        {
            Factory.Db.DropTable("People2");
            Factory.Db.DropTable("P");
            Factory.Db.DropTable("Student2");
            Factory.Db.DropTable("S");
        }

        [Test]
        public void ShowTables()
        {
            var data = Factory.Db.ShowTables();
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
        public void ShowTablesStatus()
        {
            Console.WriteLine(JsonConvert.SerializeObject(Factory.Db.ShowTablesStatus()));
        }

        [Test]
        public void GetTableEntityFromDatabase()
        {
            Console.WriteLine(JsonConvert.SerializeObject(Factory.Db.GetTableEntityFromDatabase("People")));
        }

        [Test]
        public void GetTableEntitysFromDatabase()
        {
            Console.WriteLine(JsonConvert.SerializeObject(Factory.Db.GetTableEnitysFromDatabase()));
        }

        [Test]
        public void CreateClassFileFromDatabase()
        {
            Factory.Db.CreateClassFileFromDatabase("D:\\");
        }

    }
}
