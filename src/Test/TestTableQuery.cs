using Newtonsoft.Json;
using NUnit.Framework;
using System;

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
        public void Count()
        {
            var count = Factory.peopleTable.Count();
            Factory.peopleTable.Count("WHERE Name=@Name", new { Name = "李四" });
            Console.WriteLine(count);
        }

        [Test]
        public void Min()
        {
            var count = Factory.peopleTable.Min<int>("Id");
            Console.WriteLine(count);

        }

        [Test]
        public void Max()
        {
            var count = Factory.peopleTable.Max<int>("Id");
            Console.WriteLine(count);
        }

        [Test]
        public void Sum()
        {
            var count = Factory.peopleTable.Sum<int>("Id");
            Console.WriteLine(count);

        }

        [Test]
        public void Avg()
        {
            var count = Factory.peopleTable.Avg("Id");
            Console.WriteLine(count);

        }

        [Test]
        public void GetAll()
        {
            var data = Factory.peopleTable.GetAll();
            Console.WriteLine(JsonConvert.SerializeObject(data));

            var data1 = Factory.peopleTable.GetAll("Id,Name");
            Console.WriteLine(JsonConvert.SerializeObject(data1));

            var data2 = Factory.peopleTable.GetAll("Id", "Id DESC");

            Console.WriteLine(JsonConvert.SerializeObject(data2));
        }

        [Test]
        public void GetById()
        {
            var model = Factory.peopleTable.GetById(1);
            Console.WriteLine(JsonConvert.SerializeObject(model));

            var model2 = Factory.peopleTable.GetById(1, "Id,Name,Text");
            Console.WriteLine(JsonConvert.SerializeObject(model2));

            var model3 = Factory.peopleTable.GetByIdDynamic(1, "Id,Name,Text");
            Console.WriteLine(JsonConvert.SerializeObject(model3));
        }

        [Test]
        public void GetByIdForUpdate()
        {
            var model = Factory.peopleTable.GetByIdForUpdate(1);
            Console.WriteLine(JsonConvert.SerializeObject(model));

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
            var list = Factory.peopleTable.GetByWhere("WHERE Id>@Id", new { Id = 8 }, limit: 10);
            Console.WriteLine(JsonConvert.SerializeObject(list));

            var list2 = Factory.peopleTable.GetByWhere("WHERE Id>@Id", new { Id = 8 }, "Id,Name", "ID DESC", limit: 10);
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
            var list = Factory.peopleTable.GetByPageAndCount(1, 2);
            Console.WriteLine(list.Count);
            Console.WriteLine(JsonConvert.SerializeObject(list.Data));

            var list2 = Factory.peopleTable.GetByPageAndCount(1, 2, "WHERE Id=@Id", new { Id = 1 });
            Console.WriteLine(list2.Count);
            Console.WriteLine(JsonConvert.SerializeObject(list2.Data));

            var list3 = Factory.peopleTable.GetByPageAndCount(1, 2, "WHERE Id=@Id", new { Id = 1 }, "Id,Name");
            Console.WriteLine(list3.Count);
            Console.WriteLine(JsonConvert.SerializeObject(list3.Data));
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

            var data5 = Factory.peopleTable.GetByAscLastPage(2, null);
            Console.WriteLine(JsonConvert.SerializeObject(data5));
        }

        [Test]
        public void GetByDescPage()
        {
            var data1 = Factory.peopleTable.GetByDescFirstPage(1, null);
            Console.WriteLine(JsonConvert.SerializeObject(data1));

            var data2 = Factory.peopleTable.GetByDescPrevPage(1, new People { Id = 19 });
            Console.WriteLine(JsonConvert.SerializeObject(data2));

            var data3 = Factory.peopleTable.GetByDescCurrentPage(1, new People { Id = 19 });
            Console.WriteLine(JsonConvert.SerializeObject(data3));

            var data4 = Factory.peopleTable.GetByDescNextPage(1, new People { Id = 19 });
            Console.WriteLine(JsonConvert.SerializeObject(data4));

            var data5 = Factory.peopleTable.GetByDescLastPage(1, null);
            Console.WriteLine(JsonConvert.SerializeObject(data5));
        }

    }
}
