using Dapper;
using Dapper.Sharding;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Test
{
    class TestDataBase
    {
        [Test]
        public void Add()
        {
            var p1 = Factory.Db.GetTable<People>("People", null);
            var p2 = Factory.Db.GetTable<People>("People2", null);
            var p3 = Factory.Db.GetTable<Student>("Student", null);

            Factory.Db.Using(conn =>
            {
                conn.Execute("INSERT INTO People(Name,Age)VALUES(\"阿萨的1\",288)");
                conn.Execute($"INSERT INTO Student(Id,Name,Age)VALUES(\"{ObjectId.GenerateNewIdAsString()}\",\"王\",288)");
            });

            Assert.Pass("111");
        }
    }
}
