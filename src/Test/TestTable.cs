using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Dapper.Sharding;
using Dapper;

namespace Test
{
    class TestTable
    {
        [Test]
        public void Add()
        {
            var client = DapperFactory.CreateMySqlClient("server=127.0.0.1;user=root");
            client.AutoCompareTableColumn = true;
            var db = client.GetDatabase("test");

            var p1 = db.GetTable<People>("People", null);
            var p2 = db.GetTable<People>("PPP", null);
            var p3 = db.GetTable<People2>("People2", null);

            db.Using(conn =>
            {
                conn.Execute("INSERT INTO People(Name,Age)VALUES(\"阿萨的\",288)");
                conn.Execute($"INSERT INTO People2(Id,Name,Age)VALUES(\"{ObjectId.GenerateNewIdAsString()}\",\"王\",288)");
            });

            Assert.Pass("111");
        }
    }


}
