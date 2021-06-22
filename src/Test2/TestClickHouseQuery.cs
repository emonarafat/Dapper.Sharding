using Newtonsoft.Json;
using NUnit.Framework;
using System.Collections.Generic;

namespace Test2
{
    class TestClickHouseQuery
    {
        [Test]
        public void GetAll()
        {
            var data = DbHelper.treeTable.GetAll();
            Assert.Pass(JsonConvert.SerializeObject(data));
        }

        [Test]
        public void GetById()
        {
            var data = DbHelper.treeTable.GetById("1");
            Assert.Pass(JsonConvert.SerializeObject(data));
        }

        [Test]
        public void GetByWhere()
        {
            var data = DbHelper.treeTable.GetByWhere("WHERE id=id", new { id = "6" });
            Assert.Pass(JsonConvert.SerializeObject(data));
        }

        [Test]
        public void GetByIds()
        {
            var ids = new List<string> { "0", "1" };
            var data = DbHelper.treeTable.GetByIds(ids);
            Assert.Pass(JsonConvert.SerializeObject(data));
        }

        [Test]
        public void GetByPageAndCount()
        {
            var data = DbHelper.treeTable.GetByPageAndCount(1, 5);
            Assert.Pass(JsonConvert.SerializeObject(data));
        }

        [Test]
        public void Sum()
        {
            var data = DbHelper.treeTable.Sum<long>("age");
            Assert.Pass(data.ToString());
        }
    }
}
