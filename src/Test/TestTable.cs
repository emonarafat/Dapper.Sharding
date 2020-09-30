using Dapper.Sharding;
using NUnit.Framework;
using System;
using System.Collections.Generic;
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
                var tableTran = table.CreateTranTable(conn, tran);
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
        public void BulkInsert()
        {
            //var modelList = new List<People>();
            //for (int i = 0; i < 5000; i++)
            //{
            //    modelList.Add(new People { Name = "李白" + i });
            //}
            //Factory.peopleTable.BulkInsert(modelList);

            var list = new List<Student>();
            for (int i = 0; i < 100000; i++)
            {
                list.Add(new Student { Id = ShardingFactory.NextObjectId(), Name ="李四"+i,Age = i });
            }
            Factory.studentTable.BulkInsert(list);

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
        public void InsertIdentityMany()
        {
            var modelList = new List<People>
            {
                new People{ Id = 17, Name = "李白17" },
                new People{ Id = 18,Name = "李白18" },
                new People{ Id = 19,Name = "李白19" },
                new People{ Id = 20,Name = "李白20" },
                new People{ Id = 21, Name = "李白21" },
                new People{ Id = 22,Name = "李白22" },
                new People{ Id = 23,Name = "李白23" }
            };
            var data = Factory.peopleTable.InsertIdentityMany(modelList);
            Console.WriteLine(data);
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
        public void UpdateMany()
        {
            var modelList = new List<People>
            {
                new People{ Id=22,Name="小黑" },
                new People{ Id=23,Name="小白" }
            };
            var data = Factory.peopleTable.UpdateMany(modelList);
            Console.WriteLine(data);
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
            Factory.peopleTable.UpdateInclude(model, "Money,AddTime");
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
        public void DeleteByIds()
        {
            Factory.peopleTable.DeleteByIds(new int[] { 5, 6, 7 });
        }

        [Test]
        public void DeleteByWhere()
        {
            Factory.peopleTable.DeleteByWhere("WHERE Age=@Age", new { Age = 44 });
        }

        [Test]
        public void DeleteAll()
        {
            Factory.studentTable.DeleteAll();
        }



    }
}
