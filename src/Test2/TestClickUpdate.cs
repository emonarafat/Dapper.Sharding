using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Test2
{
    class TestClickUpdate
    {
        [Test]
        public void Update()
        {
            var model = new TreeTable
            {
                id = "0",
                date = DateTime.Now,
                time = DateTime.Now,
                age = 10,
            };

            DbHelper.treeTable.Update(model, new() { "time", "age" });
            DbHelper.treeTable.Optimize();
        }

        [Test]
        public void UpdateIgnore()
        {
            var model = new TreeTable
            {
                id = "0",
                date = DateTime.Now,
                time = DateTime.Now,
                age = 10,
            };

            DbHelper.treeTable.UpdateIgnore(model, new() { "date" });
            DbHelper.treeTable.Optimize();
        }

        [Test]
        public void UpdateByWhere()
        {
            var model = new TreeTable
            {
                id = "5",
                date = DateTime.Now,
                time = DateTime.Now,
                age = 10,
            };
            string where = "WHERE id=@id";
            DbHelper.treeTable.UpdateByWhere(model, where, new() { "age" });
            DbHelper.treeTable.Optimize();
        }

        [Test]
        public void UpdateByWhere2()
        {
            var model = new TreeTable
            {
                id = "6",
                date = DateTime.Now,
                time = DateTime.Now,
                age = 10,
            };
            string where = "WHERE id=@id";
            DbHelper.treeTable.UpdateByWhere(where, model, new() { "age", "time" });
            DbHelper.treeTable.Optimize();
        }
    }
}
