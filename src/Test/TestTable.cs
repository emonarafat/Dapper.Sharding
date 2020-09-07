using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Test.Com;

namespace Test
{
    class TestTable
    {
        [Test]
        public void Insert()
        {
            var p = new People
            {
                Name = "李四",
                Age = 50,
                AddTime = DateTime.Now,
                IsAdmin = true,
                Text = "你好"
            };
            Factory.peopleTable.Insert(p);
            Console.WriteLine(p.Id);

            var teacher = new Teacher
            {
                Name = "王老师",
                Sex = 70,
                Age = 5
            };
            Factory.teacherTable.Insert(teacher);
            Console.WriteLine(teacher.Id);

            var student = new Student
            {
                Name = "李同学",
                Age = 100
            };
            Factory.studentTable.Insert(student);
            Console.WriteLine(student.Id);
        }

        [Test]
        public void InsertIdentity()
        {

            var table = new People
            {
                Id = 10,
                Name = "自动添加id"
            };
            Factory.peopleTable.InsertIdentity(table);
        }

        [Test]
        public void InsertUsingTran()
        {
            Factory.Db.UsingTran((conn, tran) =>
            {
                var table = Factory.Db.GetTable<People>("People", conn, tran);
                try
                {
                    table.Insert(null);
                    throw new Exception("an exception");
                    tran.Commit();
                }
                catch
                {
                    tran.Rollback();
                }
            });
        }
    }
}
