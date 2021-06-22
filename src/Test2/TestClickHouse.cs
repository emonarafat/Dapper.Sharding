using Dapper.Sharding;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Test2
{
    class TestClickHouse
    {

        [Test]
        public void CreateDatabase()
        {
            DbHelper.ClientHouse.CreateDatabase("test");
            Assert.Pass("ok");
        }

        [Test]
        public void DropTable()
        {
            DbHelper.Db.DropTable("people");
            DbHelper.Db.DropTable("student");
            DbHelper.Db.DropTable("teacher");
        }

        [Test]
        public void CreateCsFile()
        {
            DbHelper.Db.GeneratorClassFile("D:\\AClass");
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
                Time1 = DateTime.Now,
                Time2 = DateTime.Now,
                IsAdmin = 1,
                Money = 10.5M

            };
            DbHelper.peopleTable.Insert(p);
            Console.WriteLine(p.Id);

            var teacher = new Teacher
            {
                Id = ShardingFactory.NextSnowId(),
                Name = "王老师",
                Age = 5
            };
            DbHelper.teacherTable.Insert(teacher);
            Console.WriteLine(teacher.Id);

            var student = new Student
            {
                Id = ShardingFactory.NextObjectId(),
                Name = "李同学",
                Age = 100
            };
            DbHelper.studentTable.Insert(student);
            Console.WriteLine(student.Id);

        }

        [Test]
        public void InsertField()
        {
            var p = new People
            {
                Id = DateTime.Now.Millisecond,
                Name = "李四",
                Age = 50,
                AddTime = DateTime.Now,
                Time1 = DateTime.Now,
                Time2 = DateTime.Now,
                IsAdmin = 1,
                Money = 10.5M

            };

            var teacher = new Teacher
            {
                Id = ShardingFactory.NextSnowId(),
                Name = "王老师",
                Age = 5
            };


            var student = new Student
            {
                Id = ShardingFactory.NextObjectId(),
                Name = "李同学",
                Age = 100
            };

            DbHelper.peopleTable.Insert(p, new List<string> { "Id" });
            DbHelper.teacherTable.Insert(teacher, new List<string> { "Id" });
            DbHelper.studentTable.Insert(student, new List<string> { "Id" });
            Console.WriteLine(teacher.Id);
            Console.WriteLine(p.Id);
            Console.WriteLine(student.Id);

        }

        [Test]
        public void InsertIgnore()
        {
            var p = new People
            {
                Id = DateTime.Now.Millisecond,
                Name = "李四",
                Age = 50,
                AddTime = DateTime.Now,
                Time1 = DateTime.Now,
                Time2 = DateTime.Now,
                IsAdmin = 1,
                Money = 10.5M

            };

            var teacher = new Teacher
            {
                Id = ShardingFactory.NextSnowId(),
                Name = "王老师",
                Age = 5
            };

            var student = new Student
            {
                Id = ShardingFactory.NextObjectId(),
                Name = "李同学",
                Age = 100
            };

            DbHelper.peopleTable.InsertIgnore(p, new() { "Name" });
            DbHelper.teacherTable.InsertIgnore(teacher, new() { "Name" });
            DbHelper.studentTable.InsertIgnore(student, new() { "Name" });
            Console.WriteLine(teacher.Id);
            Console.WriteLine(p.Id);
            Console.WriteLine(student.Id);

        }

        [Test]
        public void InsertMany()
        {
            var pList = new List<People>();
            var sList = new List<Student>();
            var tList = new List<Teacher>();
            for (int i = 0; i < 10; i++)
            {
                var p = new People
                {
                    Id = DateTime.Now.Millisecond,
                    Name = "李四" + i,
                    Age = 50 + i,
                    AddTime = DateTime.Now,
                    Time1 = DateTime.Now,
                    Time2 = DateTime.Now,
                    IsAdmin = 1,
                    Money = 10.5M + i

                };
                pList.Add(p);

                var teacher = new Teacher
                {
                    Id = ShardingFactory.NextSnowId(),
                    Name = "王老师" + i,
                    Age = i
                };
                tList.Add(teacher);

                var student = new Student
                {
                    Id = ShardingFactory.NextObjectId(),
                    Name = "李同学" + i,
                    Age = i
                };
                sList.Add(student);
            }
            Stopwatch sw = new();
            sw.Start();
            DbHelper.peopleTable.Insert(pList);
            DbHelper.studentTable.Insert(sList);
            DbHelper.teacherTable.Insert(tList);
            sw.Stop();
            Assert.Pass($"毫秒数:{sw.ElapsedMilliseconds}");
        }

        [Test]
        public void InsertManyField()
        {
            var pList = new List<People>();
            var sList = new List<Student>();
            var tList = new List<Teacher>();
            for (int i = 0; i < 10; i++)
            {
                var p = new People
                {
                    Id = DateTime.Now.Millisecond,
                    Name = "李四" + i,
                    Age = 50 + i,
                    AddTime = DateTime.Now,
                    Time1 = DateTime.Now,
                    Time2 = DateTime.Now,
                    IsAdmin = 1,
                    Money = 10.5M + i

                };
                pList.Add(p);

                var teacher = new Teacher
                {
                    Id = ShardingFactory.NextSnowId(),
                    Name = "王老师" + i,
                    Age = i
                };
                tList.Add(teacher);

                var student = new Student
                {
                    Id = ShardingFactory.NextObjectId(),
                    Name = "李同学" + i,
                    Age = i
                };
                sList.Add(student);
            }

            DbHelper.peopleTable.Insert(pList, new List<string> { "Id" });
            DbHelper.studentTable.Insert(sList, new List<string> { "Id" });
            DbHelper.teacherTable.Insert(tList, new List<string> { "Id" });

            Assert.Pass();
        }

        [Test]
        public void InsertManyIgnore()
        {
            var pList = new List<People>();
            var sList = new List<Student>();
            var tList = new List<Teacher>();
            for (int i = 0; i < 10; i++)
            {
                var p = new People
                {
                    Id = DateTime.Now.Millisecond,
                    Name = "李四" + i,
                    Age = 50 + i,
                    AddTime = DateTime.Now,
                    Time1 = DateTime.Now,
                    Time2 = DateTime.Now,
                    IsAdmin = 1,
                    Money = 10.5M + i

                };
                pList.Add(p);

                var teacher = new Teacher
                {
                    Id = ShardingFactory.NextSnowId(),
                    Name = "王老师" + i,
                    Age = i
                };
                tList.Add(teacher);

                var student = new Student
                {
                    Id = ShardingFactory.NextObjectId(),
                    Name = "李同学" + i,
                    Age = i
                };
                sList.Add(student);
            }

            DbHelper.peopleTable.InsertIgnore(pList, new() { "Name" });
            DbHelper.studentTable.InsertIgnore(sList, new() { "Name" });
            DbHelper.teacherTable.InsertIgnore(tList, new() { "Name" });

            Assert.Pass();
        }

        [Test]
        public void TestSharding()
        {
            var p1 = DbHelper.Db.GetTable<Student>("p1");
            var p2 = DbHelper.Db.GetTable<Student>("p2");
            var p3 = DbHelper.Db.GetTable<Student>("p3");

            var shard = ShardingFactory.CreateShardingAuto(p1, p2, p3);
            var list = new List<Student>();
            for (int i = 0; i < 20; i++)
            {
                var p = new Student
                {
                    Id = ShardingFactory.NextObjectId(),
                    Name = "李四" + i
                };
                list.Add(p);
            }

            shard.Insert(list);
            Assert.Pass();
        }
    }
}
