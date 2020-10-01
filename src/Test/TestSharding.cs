using Dapper.Sharding;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Test.Com;

namespace Test
{
    class TestSharding
    {
        [Test]
        public void BulkInsert()
        {
            var list = new List<Student>();
            for (int i = 0; i < 10000; i++)
            {
                list.Add(new Student { Id = ShardingFactory.NextObjectId(), Name = "李四" + i, Age = i });
            }
            Factory.ShardingHash.Insert(list);

            //var list2 = new List<Teacher>();
            //for (int i = 0; i < 100000; i++)
            //{
            //    list2.Add(new Teacher { Id = i, Name = "李四" + i, Age = i });
            //}
            //Factory.ShardingRange.BulkInsert(list2);
        }
    }
}
