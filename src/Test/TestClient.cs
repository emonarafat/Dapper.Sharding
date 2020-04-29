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
            Factory.GetClient().CreateDatabase(Factory.Database);
            Assert.Pass();
        }

        [Test]
        public void DropDataBase()
        {
            Factory.GetClient().DropDatabase(Factory.Database);
            Assert.Pass();
        }

        [Test]
        public void GetAllDatabase()
        {
            var data = Factory.GetClient().GetAllDatabase();
            Console.WriteLine(JsonConvert.SerializeObject(data));
        }

        [Test]
        public void GetDataBase()
        {
            Factory.GetClient().Charset = "utf8mb4";
            Factory.GetClient().GetDatabase(Factory.Database);
            Console.WriteLine("GetDataBase");
        }

    }
}
