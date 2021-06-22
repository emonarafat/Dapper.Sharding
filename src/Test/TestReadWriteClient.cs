using Newtonsoft.Json;
using NUnit.Framework;
using System;

namespace Test
{
    class TestReadWriteClient
    {
        [Test]
        public void GetById()
        {
            var dbRead = Factory.RWClient.GetReadDatabase("test");
            var dbWrite = Factory.RWClient.GetWriteDataBase("test");
            var model = dbRead.GetTable<People>("People").GetById(1);
            var model2 = dbWrite.GetTable<People>("People").GetById(1);

            var model3 = Factory.RWClient.GetReadTable<People>("People", "test").GetById(1);

            Console.WriteLine(JsonConvert.SerializeObject(model));
            Console.WriteLine(JsonConvert.SerializeObject(model2));
            Console.WriteLine(JsonConvert.SerializeObject(model3));
        }
    }
}
