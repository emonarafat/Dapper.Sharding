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
            var peopleTable = Factory.Db.GetTable<People>("People");
            var p = new People
            {
                Name = "李四",
                Age = 50,
                AddTime = DateTime.Now,
                IsAdmin = true,
                Text = "你好"
            };
            peopleTable.Insert(p);
            Console.WriteLine(p.Id);

            var teacherTable = Factory.Db.GetTable<Teacher>("Teacher");
            var teacher = new Teacher
            {
                Name = "王老师",
                Sex = 70,
                Age = 5
            };
            teacherTable.Insert(teacher);
            Console.WriteLine(teacher.Id);

            var studentTable = Factory.Db.GetTable<Student>("Student");
            var student = new Student
            {
                Name = "李同学",
                Age = 100
            };
            studentTable.Insert(student);
            Console.WriteLine(student.Id);
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
