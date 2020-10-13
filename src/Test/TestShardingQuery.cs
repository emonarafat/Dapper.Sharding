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
            var data = Factory.ShardingQueryStudent.ExistsAsync("a").Result;
            Console.WriteLine(data);

            var data2 = Factory.ShardingQueryStudent.ExistsAsync("5f6b031cd47126c7f4308212").Result;
            Console.WriteLine(data2);
        }

        [Test]
        public void Count()
        {
            var data = Factory.ShardingQueryStudent.CountAsync().Result;
            Console.WriteLine(data);

            var data2 = Factory.ShardingQueryStudent.CountAsync("WHERE Name=@Name", new { Name = "李四1" }).Result;
            Console.WriteLine(data2);
        }

        [Test]
        public void Min()
        {
            var data = Factory.ShardingQueryStudent.MinAsync<long>("Age").Result;
            Console.WriteLine(data);
        }

        [Test]
        public void Max()
        {
            var data = Factory.ShardingQueryStudent.MaxAsync<long>("Age").Result;
            Console.WriteLine(data);
        }

        [Test]
        public void Sum()
        {
            var data = Factory.ShardingQueryStudent.SumLongAsync("Age").Result;
            Console.WriteLine(data);

            var data2 = Factory.ShardingQueryStudent.SumDecimalAsync("Age").Result;
            Console.WriteLine(data2);
        }

        [Test]
        public void Avg()
        {
            var data = Factory.ShardingQueryStudent.AvgAsync("Age").Result;
            Console.WriteLine(data);
        }

        [Test]
        public void GetAll()
        {
            var data = Factory.ShardingQueryStudent.GetAllAsync("Age", "Age DESC").Result;
            Console.WriteLine(JsonConvert.SerializeObject(data));
        }

        [Test]
        public void GetById()
        {
            var data = Factory.ShardingQueryTeacher.GetByIdAsync(66).Result;
            Console.WriteLine(JsonConvert.SerializeObject(data));
        }

        [Test]
        public void GetByIds()
        {
            var data = Factory.ShardingQueryTeacher.GetByIdsAsync(new long[] { 1, 5, 8 }).Result;
            Console.WriteLine(JsonConvert.SerializeObject(data));
        }

        [Test]
        public void GetByIdsWithField()
        {
            var data = Factory.ShardingQueryTeacher.GetByIdsWithFieldAsync(new int[] { 1, 3, 5 }, "Age").Result;
            Console.WriteLine(JsonConvert.SerializeObject(data));
        }

        [Test]
        public void GetByWhere()
        {
            var data = Factory.ShardingQueryTeacher.GetByWhereAsync("WHERE Age>@Age", new { Age = 50 }, orderby: "Id DESC", limit: 5).Result;
            Console.WriteLine(JsonConvert.SerializeObject(data));
        }

        [Test]
        public void GetByWhereFirst()
        {
            var data = Factory.ShardingQueryTeacher.GetByWhereFirstAsync("WHERE Id=@Id", new { Id = 111111111111 }).Result;
            Console.WriteLine(JsonConvert.SerializeObject(data));
        }

        [Test]
        public void GetBySkipTake()
        {
            var data = Factory.ShardingQueryTeacher.GetBySkipTakeAsync(0, 2).Result;
            Console.WriteLine(JsonConvert.SerializeObject(data));
        }

        [Test]
        public void GetByPage()
        {
            var data = Factory.ShardingQueryTeacher.GetByPageAsync(1, 2).Result;
            Console.WriteLine(JsonConvert.SerializeObject(data));
        }

        [Test]
        public void GetByPageAndCount()
        {
            var data = Factory.ShardingQueryTeacher.GetByPageAndCountAsync(2, 8).Result;
            Console.WriteLine(data.Count);
            Console.WriteLine(JsonConvert.SerializeObject(data.Data));
        }

        [Test]
        public void GetByAscPage()
        {
            var data1 = Factory.ShardingQueryTeacher.GetByAscFirstPageAsync(2).Result;
            Console.WriteLine(JsonConvert.SerializeObject(data1));

            var data2 = Factory.ShardingQueryTeacher.GetByAscPrevPageAsync(2, new Teacher { Id = 5 }).Result;
            Console.WriteLine(JsonConvert.SerializeObject(data2));

            var data3 = Factory.ShardingQueryTeacher.GetByAscCurrentPageAsync(2, new Teacher { Id = 5 }).Result;
            Console.WriteLine(JsonConvert.SerializeObject(data3));

            var data4 = Factory.ShardingQueryTeacher.GetByAscNextPageAsync(2, new Teacher { Id = 6 }).Result;
            Console.WriteLine(JsonConvert.SerializeObject(data4));

            var data5 = Factory.ShardingQueryTeacher.GetByAscLastPageAsync(2).Result;
            Console.WriteLine(JsonConvert.SerializeObject(data5));
        }

        [Test]
        public void GetByDescPage()
        {
            var data1 = Factory.ShardingQueryTeacher.GetByDescFirstPageAsync(2).Result;
            Console.WriteLine(JsonConvert.SerializeObject(data1));

            var data2 = Factory.ShardingQueryTeacher.GetByDescPrevPageAsync(2, new Teacher { Id = 19 }).Result;
            Console.WriteLine(JsonConvert.SerializeObject(data2));

            var data3 = Factory.ShardingQueryTeacher.GetByDescCurrentPageAsync(2, new Teacher { Id = 19 }).Result;
            Console.WriteLine(JsonConvert.SerializeObject(data3));

            var data4 = Factory.ShardingQueryTeacher.GetByDescNextPageAsync(2, new Teacher { Id = 18 }).Result;
            Console.WriteLine(JsonConvert.SerializeObject(data4));

            var data5 = Factory.ShardingQueryTeacher.GetByDescLastPageAsync(2).Result;
            Console.WriteLine(JsonConvert.SerializeObject(data5));
        }

    }
}
