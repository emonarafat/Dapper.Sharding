using Dapper.Sharding;
using Newtonsoft.Json;
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
                        IsAdmin = 1,
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
                Id = DateTime.Now.Millisecond,
                Name = "李四",
                Age = 50,
                AddTime = DateTime.Now,
                IsAdmin = 1,
                Text = "你好",
                Money = 10.5M,
                Money2 = 10.888F,
                Money3 = 50.55

            };
            Factory.peopleTable.Insert(p);
            Console.WriteLine(p.Id);

            var teacher = new Teacher
            {
                Id = ShardingFactory.NextSnowFlakeId(),
                Name = "王老师",
                Age = 5
            };
            Factory.teacherTable.Insert(teacher);
            Console.WriteLine(teacher.Id);

            var student = new Student
            {
                Id = ShardingFactory.NextObjectId(),
                Name = "李同学",
                Age = 100
            };
            Factory.studentTable.Insert(student);
            Console.WriteLine(student.Id);
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
                IsAdmin = 1,
                Text = "你好"
            };
            Factory.peopleTable.InsertIfNoExists(p);
            Console.WriteLine(p.Id);
        }

        [Test]
        public void InsertList()
        {
            //var modelList = new List<People>();
            //for (int i = 0; i < 5000; i++)
            //{
            //    modelList.Add(new People { Id = i, Name = "李白" + i, AddTime = DateTime.Now });
            //}
            //Factory.peopleTable.Insert(modelList);

            var list = new List<Student>();
            for (int i = 0; i < 40000; i++)
            {
                list.Add(new Student { Id = ShardingFactory.NextObjectId(), Name = "李四" + i, Age = i });
            }
            Factory.studentTable.Insert(list);

        }

        [Test]
        public void InsertIdentity()
        {

            var table = new People
            {
                Id = 11,
                Name = "自动添加id11",
                AddTime = DateTime.Now,
                IsAdmin = 1,
                Text = "你好",
                LongText = "1",
                Money = 10.5M,
                Money2 = 10.888F,
                Money3 = 50.55
            };
            Factory.peopleTable.InsertIdentityIfNoExists(table);
        }

        [Test]
        public void InsertIdentityList()
        {
            var modelList = new List<People>
            {
                new People{ Id = 17, Name = "李白17",AddTime = DateTime.Now },
                new People{ Id = 18,Name = "李白18",AddTime = DateTime.Now },
                new People{ Id = 19,Name = "李白19",AddTime = DateTime.Now },
                new People{ Id = 20,Name = "李白20",AddTime = DateTime.Now },
                new People{ Id = 21, Name = "李白21",AddTime = DateTime.Now },
                new People{ Id = 22,Name = "李白22",AddTime = DateTime.Now },
                new People{ Id = 23,Name = "李白23",AddTime = DateTime.Now }
            };
            Factory.peopleTable.InsertIdentityIfNoExists(modelList);
        }



        [Test]
        public void Merge()
        {
            var p = new People
            {
                Id = 1,
                Name = "啊实打实的",
                Age = 222,
                AddTime = DateTime.Now,
                IsAdmin = 1,
                Text = "1昂克赛拉的就撒了看得见啊"
            };
            Factory.peopleTable.Merge(p);
            Console.WriteLine(p.Id);
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
                LongText = "2",
                Money = 500M,
                AddTime = DateTime.Now

            };

            Factory.peopleTable.Update(model);
        }

        [Test]
        public void UpdateList()
        {
            var modelList = new List<People>
            {
                new People{ Id=1,Name="小黑11" ,Age = 1,AddTime = DateTime.Now},
                new People{ Id=2,Name="小白222",Age = 2 ,AddTime = DateTime.Now}
            };
            Factory.peopleTable.Update(modelList, new List<string> { "Name" });
        }

        [Test]
        public void UpdateFields()
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
            Factory.peopleTable.Update(model, new List<string> { "Money", "AddTime" });
        }

        [Test]
        public void UpdateIgnore()
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

            Factory.peopleTable.UpdateIgnore(model, new List<string> { "Name" });
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
        public void UpdateByWhere2()
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
            Factory.peopleTable.UpdateByWhere(model, "WHERE Age=@Age", new List<string> { "Name" });
        }

        [Test]
        public void UpdateByWhere3()
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

            Factory.peopleTable.UpdateByWhereIgnore(model, "WHERE Age=@Age", new List<string> { "Name" });
        }




        [Test]
        public void Delete()
        {
            Factory.peopleTable.Delete(1);
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
