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
        public void Insert()
        {
            var student = new Student { Id = ShardingFactory.NextObjectId(), Name = "李四", Age = 1 };
            Factory.ShardingHash.Insert(student);

            var teacher = new Teacher { Id = 1, Name = "李四", Age = 1 };
            Factory.ShardingRange.Merge(teacher);
        }

        [Test]
        public void InsertList()
        {
            var list = new List<Student>();
            for (int i = 0; i < 1000; i++)
            {
                list.Add(new Student { Id = ShardingFactory.NextObjectId(), Name = "李四" + i, Age = i });
            }
            Factory.ShardingHash.Insert(list);

            //var list2 = new List<Teacher>();
            //for (int i = 0; i < 100000; i++)
            //{
            //    list2.Add(new Teacher { Id = i, Name = "李四" + i, Age = i });
            //}
            //Factory.ShardingRange.Insert(list2);
        }
    }
}
