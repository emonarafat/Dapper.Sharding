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
            //do not use tran at this method
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
        public void GetByAscPage()
        {
            var data1 = Factory.peopleTable.GetByAscFirstPage(2);
            Console.WriteLine(JsonConvert.SerializeObject(data1));

            var data2 = Factory.peopleTable.GetByAscPrevPage(2, new People { Id = 5 });
            Console.WriteLine(JsonConvert.SerializeObject(data2));

            var data3 = Factory.peopleTable.GetByAscCurrentPage(2, new People { Id = 5 });
            Console.WriteLine(JsonConvert.SerializeObject(data3));

            var data4 = Factory.peopleTable.GetByAscNextPage(2, new People { Id = 6 });
            Console.WriteLine(JsonConvert.SerializeObject(data4));

            var data5 = Factory.peopleTable.GetByAscLastPage(2);
            Console.WriteLine(JsonConvert.SerializeObject(data5));
        }

        [Test]
        public void GetByDescPage()
        {
            var data1 = Factory.peopleTable.GetByDescFirstPage(2);
            Console.WriteLine(JsonConvert.SerializeObject(data1));

            var data2 = Factory.peopleTable.GetByDescPrevPage(2, new People { Id = 19 });
            Console.WriteLine(JsonConvert.SerializeObject(data2));

            var data3 = Factory.peopleTable.GetByDescCurrentPage(2, new People { Id = 19 });
            Console.WriteLine(JsonConvert.SerializeObject(data3));

            var data4 = Factory.peopleTable.GetByDescNextPage(2, new People { Id = 18 });
            Console.WriteLine(JsonConvert.SerializeObject(data4));

            var data5 = Factory.peopleTable.GetByDescLastPage(2);
            Console.WriteLine(JsonConvert.SerializeObject(data5));
        }

    }
}
