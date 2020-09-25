using NUnit.Framework;
using System;
using System.Collections.Generic;
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
        public void InsertTeacher()
        {
            for (int i = 0; i < 100; i++)
            {
                Factory.ShardingAutoTeacher.Insert(new Teacher() { Id = i, Name = "李四" + i, Age = i });
            }

            //snowflake
            //for (int i = 0; i < 100; i++)
            //{
            //    Factory.ShardingAutoTeacher.Insert(new Teacher() { Name = "李四" + i, Age = i });
            //}
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
