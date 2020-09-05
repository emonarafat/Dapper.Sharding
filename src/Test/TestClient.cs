using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Dapper.Sharding;
using Dapper;
using Newtonsoft.Json;

namespace Test
{
    class TestClient
    {
        [Test]
        public void CreateDatabase()
        {
            Factory.Client.CreateDatabase("demo");
        }

        [Test]
        public void DropDatabase()
        {
            Factory.Client.DropDatabase("demo");
        }

        [Test]
        public void ExistDatabase()
        {
            bool exists = Factory.Client.ExistsDatabase("aaa");
            Console.WriteLine(exists);
        }

        [Test]
        public void ShowDatabases()
        {
            var databases = Factory.Client.ShowDatabases();
            Console.WriteLine(JsonConvert.SerializeObject(databases));
        }

        [Test]
        public void ShowDatabasesWithOutSystem()
        {
            var databases = Factory.Client.ShowDatabasesExcludeSystem();
            Console.WriteLine(JsonConvert.SerializeObject(databases));
        }

        [Test]
        public void GetDatbase()
        {
            Factory.Client.GetDatabase("ok");
        }

        [Test]
        public void ClearCache()
        {
            Factory.Client.ClearCache();
        }

    }


}
