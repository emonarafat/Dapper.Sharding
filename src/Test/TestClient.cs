using Dapper.Sharding;
using Newtonsoft.Json;
using NUnit.Framework;
using System;
using System.Threading.Tasks;

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
        public void GetDatbase()
        {
            Factory.Client.GetDatabase("demo");
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

        [Test]
        public void NextId()
        {
            var id = ShardingFactory.NextLongIdAsString();
            var id2 = ShardingFactory.NextObjectId();
            var id3 = ShardingFactory.NextSnowIdAsString();
            Assert.Pass($"{id}\n{id2}\n{id3}");
        }

        [Test]
        public async Task TestThreadDb()
        {
            var task1 = Task.Run(() =>
            {
                return Factory.Client.GetDatabase("zzz1");
            });

            var task2 = Task.Run(() =>
            {
                return Factory.Client.GetDatabase("zzz1");
            });

            await Task.WhenAll(task1, task2);
            Console.WriteLine(task1.Result.Equals(task2.Result));
            Console.WriteLine("没问题");
          
        }

        [Test]
        public async Task TestThreadTable()
        {
            var task1 = Task.Run(() =>
            {
                return Factory.Client.GetDatabase("zzz1").GetTable<People>("p");
            });

            var task2 = Task.Run(() =>
            {
                return Factory.Client.GetDatabase("zzz1").GetTable<People>("p");
            });

            await Task.WhenAll(task1, task2);
            Console.WriteLine(task1.Result.Equals(task2.Result));
            Console.WriteLine("没问题");
        }

    }


}
