using Dapper.Sharding;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Test2
{
    class TestClickInsertIfNotExists
    {
        [Test]
        public void Insert()
        {
            DbHelper.Db.DropTable("tree");
            var list = new List<TreeTable>();
            for (int i = 0; i < 4; i++)
            {
                var model = new TreeTable
                {
                    id = i.ToString(),
                    date = DateTime.Now.AddDays(i),
                    time = DateTime.Now.AddSeconds(i),
                    age = i
                };
                list.Add(model);
            }
            DbHelper.treeTable.Insert(list);
            DbHelper.treeTable.Optimize();
        }
        [Test]
        public void InsertNo()
        {
            var model = new TreeTable
            {
                id = "0",
                date = DateTime.Now.AddDays(0),
                time = DateTime.Now.AddSeconds(1),
                age = 1
            };

            DbHelper.treeTable.InsertIfNoExists(model);
            DbHelper.treeTable.Optimize();
        }

        [Test]
        public void InsertManyNo()
        {
            var list = new List<TreeTable>();
            for (int i = 0; i < 8; i++)
            {
                var model = new TreeTable
                {
                    id = i.ToString(),
                    date = DateTime.Now.AddDays(i),
                    time = DateTime.Now.AddSeconds(i),
                    age = i
                };
                list.Add(model);
            }
            DbHelper.treeTable.InsertIfNoExists(list);
            DbHelper.treeTable.Optimize();
        }

    }
}