using Newtonsoft.Json;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Test
{
    class TestTableQuery
    {
        [Test]
        public void Exists()
        {
            var result = Factory.peopleTable.Exists(1);
            Console.WriteLine(result);
        }

        [Test]
        public void ExistsModel()
        {
            var p = new People { Id = 11 };
            var result = Factory.peopleTable.Exists(p);
            Console.WriteLine(result);
        }

        [Test]
        public void Count()
        {
            var count = Factory.peopleTable.Count();
            Console.WriteLine(count);

            var count2 = Factory.peopleTable.Count("WHERE Id>@Id", new { Id = 10 });
            Console.WriteLine(count2);
        }

        [Test]
        public void GetAll()
        {
            var data = Factory.peopleTable.GetAll();
            Console.WriteLine(JsonConvert.SerializeObject(data));

            var data1 = Factory.peopleTable.GetAll("Id,Name");
            Console.WriteLine(JsonConvert.SerializeObject(data1));

            var data2 = Factory.peopleTable.GetAll("Id", "ORDER BY Id DESC");
            Console.WriteLine(JsonConvert.SerializeObject(data2));
        }

        [Test]
        public void GetById()
        {
            var model = Factory.peopleTable.GetById(1);
            Console.WriteLine(JsonConvert.SerializeObject(model));

            var model2 = Factory.peopleTable.GetById(1, "Id,Name,Text");
            Console.WriteLine(JsonConvert.SerializeObject(model2));
        }

        [Test]
        public void GetByIds()
        {
            var list = Factory.peopleTable.GetByIds(new long[] { 1L, 2L, 3L });
            Console.WriteLine(JsonConvert.SerializeObject(list));

            var list2 = Factory.peopleTable.GetByIds(new long[] { 1L, 2L, 3L }, "Id,Name");
            Console.WriteLine(JsonConvert.SerializeObject(list2));
        }

        [Test]
        public void GetByIdsWithField()
        {
            var list = Factory.peopleTable.GetByIdsWithField(new string[] { "2", "3" }, "Name");
            Console.WriteLine(JsonConvert.SerializeObject(list));

            var list2 = Factory.peopleTable.GetByIdsWithField(new string[] { "2", "3" }, "Name", "Id,Name");
            Console.WriteLine(JsonConvert.SerializeObject(list2));
        }

        [Test]
        public void GetByWhere()
        {
            var list = Factory.peopleTable.GetByWhere("WHERE Id>@Id", new { Id = 8 });
            Console.WriteLine(JsonConvert.SerializeObject(list));

            var list2 = Factory.peopleTable.GetByWhere("WHERE Id>@Id", new { Id = 8 }, "Id,Name");
            Console.WriteLine(JsonConvert.SerializeObject(list2));
        }

        [Test]
        public void GetByWhereFirst()
        {
            var model = Factory.peopleTable.GetByWhereFirst("WHERE Id=@Id", new { id = 8 });
            Console.WriteLine(JsonConvert.SerializeObject(model));

            var model2 = Factory.peopleTable.GetByWhereFirst("WHERE Id=@Id", new { id = 8 }, "Id,Name");
            Console.WriteLine(JsonConvert.SerializeObject(model2));
        }

        [Test]
        public void GetBySkipTake()
        {
            var list = Factory.peopleTable.GetBySkipTake(0, 2);
            Console.WriteLine(JsonConvert.SerializeObject(list));

            var list2 = Factory.peopleTable.GetBySkipTake(0, 2, "WHERE Id=@Id", new { Id = 1 });
            Console.WriteLine(JsonConvert.SerializeObject(list2));

            var list3 = Factory.peopleTable.GetBySkipTake(0, 2, "WHERE Id=@Id", new { Id = 1 }, "Id,Name");
            Console.WriteLine(JsonConvert.SerializeObject(list3));
        }

        [Test]
        public void GetByPage()
        {
            var list = Factory.peopleTable.GetByPage(1, 2);
            Console.WriteLine(JsonConvert.SerializeObject(list));

            var list2 = Factory.peopleTable.GetByPage(1, 2, "WHERE Id=@Id", new { Id = 1 });
            Console.WriteLine(JsonConvert.SerializeObject(list2));

            var list3 = Factory.peopleTable.GetByPage(1, 2, "WHERE Id=@Id", new { Id = 1 }, "Id,Name");
            Console.WriteLine(JsonConvert.SerializeObject(list3));
        }

        [Test]
        public void GetByPageAndCount()
        {
            var list = Factory.peopleTable.GetByPageAndCount(1, 2, out var total);
            Console.WriteLine(total);
            Console.WriteLine(JsonConvert.SerializeObject(list));

            var list2 = Factory.peopleTable.GetByPageAndCount(1, 2, out var total2, "WHERE Id=@Id", new { Id = 1 });
            Console.WriteLine(total2);
            Console.WriteLine(JsonConvert.SerializeObject(list2));

            var list3 = Factory.peopleTable.GetByPageAndCount(1, 2, out var total3, "WHERE Id=@Id", new { Id = 1 }, "Id,Name");
            Console.WriteLine(total3);
            Console.WriteLine(JsonConvert.SerializeObject(list3));
        }

        [Test]
        public void GetByPageAndCountTran()
        {
            //do not use this method
            Factory.Db.UsingTran((conn, tran) =>
            {
                var tb = Factory.peopleTable.BeginTran(conn, tran);
                var list = tb.GetByPageAndCount(1, 2, out var total);
                Console.WriteLine(total);
                Console.WriteLine(JsonConvert.SerializeObject(list));
            });
        }
    }
}
