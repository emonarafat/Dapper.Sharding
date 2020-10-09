using Newtonsoft.Json;
using NUnit.Framework;
using System;
using System.Threading;

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
            for (int i = 0; i < 105; i++)
            {
                Factory.Client.DropDatabase("demo" + i);

            }

        }


        [Test]
        public void GetDatbase()
        {
            for (int i = 0; i < 105; i++)
            {
                Factory.Client.GetDatabase("demo"+i).GetTable<Student>("a");
                Thread.Sleep(200);

            }
         
        }

        [Test]
        public void ExistDatabase()
        {
            bool exists = Factory.Client.ExistsDatabase("demo");
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
        public void ClearCache()
        {
            Factory.Client.ClearCache();
        }

    }


}
