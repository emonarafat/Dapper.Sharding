using Newtonsoft.Json;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Test
{
    class TestShardingQuery
    {
        [Test]
        public void Exists()
        {
            var data = Factory.ShardingQuery.Exists("a");
            Console.WriteLine(data);

            var data2 = Factory.ShardingQuery.Exists("5f6b031cd47126c7f4308212");
            Console.WriteLine(data2);
        }

        [Test]
        public void Count()
        {
            var data = Factory.ShardingQuery.Count();
            Console.WriteLine(data);

            var data2 = Factory.ShardingQuery.Count("WHERE Name=@Name", new { Name = "李四1" });
            Console.WriteLine(data2);
        }
        
        [Test]
        public void Min()
        {
            var data = Factory.ShardingQuery.Min<long>("Age");
            Console.WriteLine(data);
        }

        [Test]
        public void Max()
        {
            var data = Factory.ShardingQuery.Max<long>("Age");
            Console.WriteLine(data);
        }

        [Test]
        public void Sum()
        {
            var data = Factory.ShardingQuery.SumLong("Age");
            Console.WriteLine(data);

            var data2 = Factory.ShardingQuery.SumDecimal("Age");
            Console.WriteLine(data2);
        }

        [Test]
        public void Avg()
        {
            var data = Factory.ShardingQuery.Avg("Age");
            Console.WriteLine(data);
        }

        [Test]
        public void GetAll()
        {
            var data = Factory.ShardingQuery.GetAll();
            Console.WriteLine(JsonConvert.SerializeObject(data));
        }

    }
}
