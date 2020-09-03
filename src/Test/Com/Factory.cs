using Dapper.Sharding;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Test
{
    public class Factory
    {
        public static IClient Client = ClientFactory.CreateMySqlClient("server=127.0.0.1;user=root");
        public static IDatabase Db = Client.GetDatabase("test");
    }
}
