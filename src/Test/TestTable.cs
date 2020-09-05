using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Test
{
    class TestTable
    {
        [Test]
        public void Insert()
        {
            var table = Factory.Db.GetTable<People>("People");
            table.Insert(null);
        }

        [Test]
        public void InsertUsingTran()
        {
            Factory.Db.UsingTran((conn, tran) =>
            {
                var table = Factory.Db.GetTable<People>("People", conn, tran);
                try
                {
                    table.Insert(null);
                    throw new Exception("an exception");
                    tran.Commit();
                }
                catch 
                {
                    tran.Rollback();
                } 
            });
        }
    }
}
