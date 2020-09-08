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
        public void BeginTran()
        {
            Factory.Db.UsingTran((conn, tran) =>
            {
                var table = Factory.peopleTable;
                var tableTran = table.BeginTran(conn, tran);
                try
                {
                    var p = new People
                    {
                        Name = "李四",
                        Age = 50,
                        AddTime = DateTime.Now,
                        IsAdmin = true,
                        Text = "你好"
                    };
                    tableTran.Insert(p);
                    throw new Exception("an exception");
                    tran.Commit();
                }
                catch
                {
                    tran.Rollback();
                }

            });
        }

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
                Id = 11,
                Name = "自动添加id"
            };
            Factory.peopleTable.InsertIdentity(table);
        }

        [Test]
        public void Update()
        {
            var model = new People
            {
                Id = 1,
                Name = "李四1111",
                Age = 51111,
                Text = "你好11",
                Money = 500M,
                AddTime = DateTime.Now
            };

            Factory.peopleTable.Update(model);
        }

        [Test]
        public void UpdateInclude()
        {
            var model = new People
            {
                Id = 1,
                Name = "111",
                Age = 123,
                Text = "666",
                Money = 200M,
                AddTime = DateTime.Now
            };
            Factory.peopleTable.UpdateInclude(model, "Money");
        }

        [Test]
        public void UpdateExclude()
        {
            var model = new People
            {
                Id = 1,
                Name = "333",
                Age = 333,
                Text = "444",
                Money = 800M,
                AddTime = DateTime.Now
            };

            Factory.peopleTable.UpdateExclude(model, "Name");
        }

        [Test]
        public void UpdateByWhere()
        {
            var model = new People
            {
                Id = 1,
                Name = "李四1111",
                Age = 333,
                Text = "你好11",
                Money = 500M,
                AddTime = DateTime.Now
            };

            Factory.peopleTable.UpdateByWhere(model, "WHERE Age=@Age");
        }

        [Test]
        public void UpdateByWhereInclude()
        {
            var model = new People
            {
                Id = 1,
                Name = "111",
                Age = 333,
                Text = "666",
                Money = 200M,
                AddTime = DateTime.Now
            };
            Factory.peopleTable.UpdateByWhereInclude(model, "WHERE Age=@Age", "Name");
        }

        [Test]
        public void UpdateByWhereExclude()
        {
            var model = new People
            {
                Id = 1,
                Name = "333",
                Age = 333,
                Text = "444",
                Money = 800M,
                AddTime = DateTime.Now
            };

            Factory.peopleTable.UpdateByWhereExclude(model, "WHERE Age=@Age", "Name");
        }

        [Test]
        public void InsertIfNoExists()
        {
            var p = new People
            {
                Id = 12,
                Name = "李四",
                Age = 50,
                AddTime = DateTime.Now,
                IsAdmin = true,
                Text = "你好"
            };
            Factory.peopleTable.InsertIfNoExists(p);
            Console.WriteLine(p.Id);
        }

        [Test]
        public void InsertIfExistsUpdate()
        {
            var p = new People
            {
                Id = 12,
                Name = "asdad",
                Age = 44,
                AddTime = DateTime.Now,
                IsAdmin = true,
                Text = "你好21112222222"
            };
            Factory.peopleTable.InsertIfExistsUpdate(p, "Age");
            Console.WriteLine(p.Id);
        }

        [Test]
        public void Delete()
        {
            Factory.peopleTable.Delete(11);
        }

        [Test]
        public void DeleteModel()
        {
            Factory.peopleTable.Delete(new People { Id = 11 });
        }

        [Test]
        public void Exists()
        {
            var result = Factory.peopleTable.Exists(1);
            Console.WriteLine(result);
        }

        [Test]
        public void ExistsModel()
        {
            var p = new People { Id = 11 };
            var result = Factory.peopleTable.Exists(p);
            Console.WriteLine(result);
        }
    }
}
