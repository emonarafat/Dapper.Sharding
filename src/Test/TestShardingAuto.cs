using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Test.Com;

namespace Test
{
    class TestShardingAuto
    {
        [Test]
        public void Count()
        {
            var count = Factory.ShardingAuto.Query.Count();
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

        [Test]
        public void InsertMany()
        {
            List<Teacher> list = new List<Teacher>();
            //for (int i = 0; i < 10000; i++)
            //{
            //    list.Add(new Teacher() { Id = i + 1, Name = "李四" + i, Age = i + 1 });
            //}

            //snowflake
            for (int i = 0; i < 100000; i++)
            {
                list.Add(new Teacher() { Id = 0, Name = "李四" + i, Age = i + 1 });
            }
            Factory.ShardingAutoTeacher.InsertMany(list);
        }

        public void Tran()
        {
            var tran = Factory.ShardingAutoTeacher.BeginTran();
            try
            {
                tran.Commit();
            }
            catch
            {
                tran.Rollback();
            }

        }

    }
}
