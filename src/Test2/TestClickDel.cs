using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Test2
{
    class TestClickDel
    {
        [Test]
        public void Delete()
        {
            DbHelper.treeTable.Delete("0");
            DbHelper.Db.OptimizeTable("tree");
        }

        [Test]
        public void DeleteAll()
        {
            DbHelper.treeTable.DeleteAll();
            DbHelper.Db.OptimizeTable("tree");
        }

        [Test]
        public void DeleteByIds()
        {
            DbHelper.treeTable.DeleteByIds(new List<string> { "0", "1" });
            DbHelper.Db.OptimizeTable("tree");
        }

        [Test]
        public void DeleteByWhere()
        {
            DbHelper.treeTable.DeleteByWhere("WHERE id>@id", new { id = "0" });
            DbHelper.Db.OptimizeTable("tree");
        }

        [Test]
        public void DeleteModel()
        {
            var model = new TreeTable { id = "0" };
            DbHelper.treeTable.Delete(model);
            DbHelper.Db.OptimizeTable("tree");
        }

        [Test]
        public void DeleteList()
        {
            var list = new List<TreeTable>
            { 
                new TreeTable { id = "0" },
                new TreeTable { id = "1" }
            };
            DbHelper.treeTable.Delete(list);
            DbHelper.Db.OptimizeTable("tree");
        }
    }
}
