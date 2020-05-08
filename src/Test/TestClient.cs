using Newtonsoft.Json;
using NUnit.Framework;
using System;

namespace Test
{
    [TestFixture]
    public class TestClient
    {

        [Test]
        public void CreateDataBase()
        {
            Factory.Client.CreateDatabase(Factory.Database);
            Assert.Pass();
        }

        [Test]
        public void DropDataBase()
        {
            Factory.Client.DropDatabase(Factory.Database);
            Assert.Pass();
        }

        [Test]
        public void GetAllDatabase()
        {
            var data = Factory.Client.GetAllDatabase();
            Console.WriteLine(JsonConvert.SerializeObject(data));
        }

        [Test]
        public void GetDataBase()
        {
            Factory.Client.Charset = "utf8mb4";
            Factory.GetDatabase();
            Console.WriteLine("GetDataBase");
        }

    }
}
