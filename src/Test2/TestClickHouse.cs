using Dapper.Sharding;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Test2
{
    class TestClickHouse
    {
        public static IDatabase db;
        public static ITable<People> tablePe;
        public static ITable<Student> tableStu;
        public static ITable<Teacher> tableTc;

        [SetUp]
        public void Start()
        {
            db = DbHelper.ClientHouse.GetDatabase("test");
            tablePe = db.GetTable<People>("people");
            tableStu = db.GetTable<Student>("student");
            tableTc = db.GetTable<Teacher>("teacher");
        }

        [Test]
        public void CreateDatabase()
        {
            DbHelper.ClientHouse.CreateDatabase("test");
            Assert.Pass("ok");
        }

        [Test]
        public void DropTable()
        {
            db.DropTable("people");
            db.DropTable("student");
            db.DropTable("teacher");
        }

        [Test]
        public void CreateCsFile()
        {
            db.GeneratorClassFile("D:\\AClass");
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
            tablePe.Insert(p);
            Console.WriteLine(p.Id);

            var teacher = new Teacher
            {
                Id = ShardingFactory.NextSnowId(),
                Name = "王老师",
                Age = 5
            };
            tableTc.Insert(teacher);
            Console.WriteLine(teacher.Id);

            var student = new Student
            {
                Id = ShardingFactory.NextObjectId(),
                Name = "李同学",
                Age = 100
            };
            tableStu.Insert(student);
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

            tablePe.Insert(p, new() { "Id" });
            tableTc.Insert(teacher, new() { "Id" });    
            tableStu.Insert(student, new() { "Id" });
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

            tablePe.InsertIgnore(p, new() { "Name" });
            tableTc.InsertIgnore(teacher, new() { "Name" });
            tableStu.InsertIgnore(student, new() { "Name" });
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

            tablePe.Insert(pList);
            tableStu.Insert(sList);
            tableTc.Insert(tList);

            Assert.Pass();
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

            tablePe.Insert(pList, new() { "Id" });
            tableStu.Insert(sList, new() { "Id" });
            tableTc.Insert(tList, new() { "Id" });

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

            tablePe.InsertIgnore(pList, new() { "Name" });
            tableStu.InsertIgnore(sList, new() { "Name" });
            tableTc.InsertIgnore(tList, new() { "Name" });

            Assert.Pass();
        }
    }
}
