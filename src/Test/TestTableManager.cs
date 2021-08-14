using Dapper.Sharding;
using Newtonsoft.Json;
using NUnit.Framework;
using System;

namespace Test
{
    class TestTableManager
    {
        [Test]
        public void CreateIndex()
        {
            Factory.TableManager.CreateIndex("Name", "Name", IndexType.Normal);
            Factory.TableManager.CreateIndex("Age", "Age", IndexType.Unique);
            Factory.TableManager.CreateIndex("NameAndAge", "Name,Age", IndexType.Unique);
        }

        [Test]
        public void DropIndex()
        {
            Factory.TableManager.DropIndex("Name");
            Factory.TableManager.DropIndex("Age");
            Factory.TableManager.DropIndex("NameAndAge");
        }

        [Test]
        public void GetIndexEntityList()
        {
            var data = Factory.TableManager.GetIndexEntityList();
            Console.WriteLine(JsonConvert.SerializeObject(data));
        }

        [Test]
        public void GetColumnEntityList()
        {
            var data = Factory.TableManager.GetColumnEntityList();
            Console.WriteLine(JsonConvert.SerializeObject(data));
        }

        [Test]
        public void AddColumn()
        {
            Factory.TableManager.AddColumn("NewColumn", typeof(string), 60, "新增字段stting");
            Factory.TableManager.AddColumn("NewColumn2", typeof(int), 0, "新增字段int");
        }

        [Test]
        public void DropColumn()
        {
            Factory.TableManager.DropColumn("NewColumn");
            Factory.TableManager.DropColumn("NewColumn2");
        }

    }
}
