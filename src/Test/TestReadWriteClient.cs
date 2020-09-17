using Newtonsoft.Json;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Test
{
    class TestReadWriteClient
    {
        [Test]
        public void GetById()
        {
            var read = Factory.RWClient.ReadClient;
            var write = Factory.RWClient.WriteClient;
            var model = read.GetDatabase("test").GetTable<People>("People").GetById(1);
            var model2 = write.GetDatabase("test").GetTable<People>("People").GetById(1);

            Console.WriteLine(JsonConvert.SerializeObject(model));
            Console.WriteLine(JsonConvert.SerializeObject(model2));
        }
    }
}
