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
    class TestClient
    {

        [Test]
        public void ExistDatabase()
        {
            bool exists = Factory.Client.ExistsDatabase("testasd");
            Console.WriteLine(exists);
        }

       


    }


}
