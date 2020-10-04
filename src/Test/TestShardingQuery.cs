using Newtonsoft.Json;
using NUnit.Framework;
using System;
using Test.Com;

namespace Test
{
    class TestShardingQuery
    {
        [Test]
        public void Exists()
        {
            var data = Factory.ShardingQueryStudent.Exists("a");
            Console.WriteLine(data);

            var data2 = Factory.ShardingQueryStudent.Exists("5f6b031cd47126c7f4308212");
            Console.WriteLine(data2);
        }

        [Test]
        public void Count()
        {
            var data = Factory.ShardingQueryStudent.Count();
            Console.WriteLine(data);

            var data2 = Factory.ShardingQueryStudent.Count("WHERE Name=@Name", new { Name = "李四1" });
            Console.WriteLine(data2);
        }

        [Test]
        public void Min()
        {
            var data = Factory.ShardingQueryStudent.Min<long>("Age");
            Console.WriteLine(data);
        }

        [Test]
        public void Max()
        {
            var data = Factory.ShardingQueryStudent.Max<long>("Age");
            Console.WriteLine(data);
        }

        [Test]
        public void Sum()
        {
            var data = Factory.ShardingQueryStudent.SumLong("Age");
            Console.WriteLine(data);

            var data2 = Factory.ShardingQueryStudent.SumDecimal("Age");
            Console.WriteLine(data2);
        }

        [Test]
        public void Avg()
        {
            var data = Factory.ShardingQueryStudent.Avg("Age");
            Console.WriteLine(data);
        }

        [Test]
        public void GetAll()
        {
            var data = Factory.ShardingQueryStudent.GetAll("Age", "Age DESC");
            Console.WriteLine(JsonConvert.SerializeObject(data));
        }

        [Test]
        public void GetById()
        {
            var data = Factory.ShardingQueryTeacher.GetById(66);
            Console.WriteLine(JsonConvert.SerializeObject(data));
        }

        [Test]
        public void GetByIds()
        {
            var data = Factory.ShardingQueryTeacher.GetByIds(new long[] { 1, 5, 8 });
            Console.WriteLine(JsonConvert.SerializeObject(data));
        }

        [Test]
        public void GetByIdsWithField()
        {
            var data = Factory.ShardingQueryTeacher.GetByIdsWithField(new int[] { 1, 3, 5 }, "Age");
            Console.WriteLine(JsonConvert.SerializeObject(data));
        }

        [Test]
        public void GetByWhere()
        {
            var data = Factory.ShardingQueryTeacher.GetByWhere("WHERE Age>@Age", new { Age = 50 }, orderby: "Id DESC", limit: 5);
            Console.WriteLine(JsonConvert.SerializeObject(data));
        }

        [Test]
        public void GetByWhereFirst()
        {
            var data = Factory.ShardingQueryTeacher.GetByWhereFirst("WHERE Id=@Id", new { Id = 111111111111 });
            Console.WriteLine(JsonConvert.SerializeObject(data));
        }

        [Test]
        public void GetBySkipTake()
        {
            var data = Factory.ShardingQueryTeacher.GetBySkipTake(0, 2);
            Console.WriteLine(JsonConvert.SerializeObject(data));
        }

        [Test]
        public void GetByPage()
        {
            var data = Factory.ShardingQueryTeacher.GetByPage(1, 2);
            Console.WriteLine(JsonConvert.SerializeObject(data));
        }

        [Test]
        public void GetByPageAndCount()
        {
            var data = Factory.ShardingQueryTeacher.GetByPageAndCount(2, 8, out var count);
            Console.WriteLine(count);
            Console.WriteLine(JsonConvert.SerializeObject(data));
        }

        [Test]
        public void GetByAscPage()
        {
            var data1 = Factory.ShardingQueryTeacher.GetByAscFirstPage(2);
            Console.WriteLine(JsonConvert.SerializeObject(data1));

            var data2 = Factory.ShardingQueryTeacher.GetByAscPrevPage(2, new Teacher { Id = 5 });
            Console.WriteLine(JsonConvert.SerializeObject(data2));

            var data3 = Factory.ShardingQueryTeacher.GetByAscCurrentPage(2, new Teacher { Id = 5 });
            Console.WriteLine(JsonConvert.SerializeObject(data3));

            var data4 = Factory.ShardingQueryTeacher.GetByAscNextPage(2, new Teacher { Id = 6 });
            Console.WriteLine(JsonConvert.SerializeObject(data4));

            var data5 = Factory.ShardingQueryTeacher.GetByAscLastPage(2);
            Console.WriteLine(JsonConvert.SerializeObject(data5));
        }

        [Test]
        public void GetByDescPage()
        {
            var data1 = Factory.ShardingQueryTeacher.GetByDescFirstPage(2);
            Console.WriteLine(JsonConvert.SerializeObject(data1));

            var data2 = Factory.ShardingQueryTeacher.GetByDescPrevPage(2, new Teacher { Id = 19 });
            Console.WriteLine(JsonConvert.SerializeObject(data2));

            var data3 = Factory.ShardingQueryTeacher.GetByDescCurrentPage(2, new Teacher { Id = 19 });
            Console.WriteLine(JsonConvert.SerializeObject(data3));

            var data4 = Factory.ShardingQueryTeacher.GetByDescNextPage(2, new Teacher { Id = 18 });
            Console.WriteLine(JsonConvert.SerializeObject(data4));

            var data5 = Factory.ShardingQueryTeacher.GetByDescLastPage(2);
            Console.WriteLine(JsonConvert.SerializeObject(data5));
        }

    }
}
