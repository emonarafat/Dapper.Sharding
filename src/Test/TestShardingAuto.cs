using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Test
{
    class TestShardingAuto
    {
        [Test]
        public void Count()
        {
            var count = Factory.ShardingAuto.Count();
            Console.WriteLine(count);
        }

        [Test]
        public void Insert()
        {
            for (int i = 0; i < 100; i++)
            {
                Factory.ShardingAuto.Insert(new Student() { Name = "李四" + i, Age = i });
            }
        }

    }
}
