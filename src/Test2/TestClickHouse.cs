using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Test2
{
    class TestClickHouse
    {
        [Test]
        public void CreateDatabase()
        {
            DbHelper.ClientHouse.CreateDatabase("test");
            Assert.Pass("ok");
        }
    }
}
