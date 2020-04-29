using Newtonsoft.Json;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Dapper.Sharding;

namespace Test
{
    public class TestTableManager
    {
        [Test]
        public void GetColumns()
        {
            var data = Factory.GetTableManager().GetColumns();
            Console.WriteLine(JsonConvert.SerializeObject(data));
        }

        [Test]
        public void CreateIndex()
        {
            Factory.GetTableManager().CreateIndex("name22", "`Fl`,`Db`", IndexType.Unique);
        }

        [Test]
        public void DropIndex()
        {
            Factory.GetTableManager().DropIndex("name");
        }

        [Test]
        public void AlertIndex()
        {
            Factory.GetTableManager().AlertIndex("name", "name", IndexType.Unique);
        }

        [Test]
        public void GetIndexs()
        {
            var data = Factory.GetTableManager().GetIndexs();
            Console.WriteLine(JsonConvert.SerializeObject(data));
        }

        [Test]
        public void GetIndexEntitys()
        {
            var data = Factory.GetTableManager().GetIndexEntitys();
            Console.WriteLine(JsonConvert.SerializeObject(data));
        }

        [Test]
        public void GetColumnEntitys()
        {
            var data = Factory.GetTableManager().GetColumnEntitys();
            Console.WriteLine(JsonConvert.SerializeObject(data));
        }

        [Test]
        public void Rename()
        {
            Factory.GetTableManager().Rename("School");
        }

        [Test]
        public void SetComment()
        {
            Factory.GetTableManager().SetComment("人类");
        }

        [Test]
        public void SetCharset()
        {
            Factory.GetTableManager().SetCharset("utf8mb4");
        }

        [Test]
        public void AddColumn()
        {
            Factory.GetTableManager().AddColumn("Hi", typeof(decimal), 11.5, "嘿嘿d");
        }

        [Test]
        public void DropColumn()
        {
            Factory.GetTableManager().DropColumn("Hi");
        }

        [Test]
        public void AddColumnAfter()
        {
            Factory.GetTableManager().AddColumnAfter("Hii","Id", typeof(double), 11.5, "嘿嘿d");
        }

        [Test]
        public void AddColumnFirst()
        {
            Factory.GetTableManager().AddColumnFirst("Hiii2", typeof(double), 10, "");
        }

        [Test]
        public void ModifyColumn()
        {
            Factory.GetTableManager().ModifyColumn("Hiii2", typeof(int), 5, "哎");
        }

        [Test]
        public void ModifyColumnFirst()
        {
            Factory.GetTableManager().ModifyColumnFirst("Db", typeof(int), 5, "哎111222");
        }

        [Test]
        public void ModifyColumnAfter()
        {
            Factory.GetTableManager().ModifyColumnAfter("Hiii2", "Id", typeof(int), 5, "哎111222");
        }

        [Test]
        public void ModifyColumnName()
        {
            Factory.GetTableManager().ModifyColumnName("Name","Name",typeof(string),10,"新的名字");
        }

    }
}
