using Dapper.Sharding;
using Newtonsoft.Json;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Test
{
    class TestTableManager
    {
        [Test]
        public void CreateIndex()
        {
            Factory.TableManager.CreateIndex("Name", "Name", IndexType.Normal);
            Factory.TableManager.CreateIndex("Age", "Age", IndexType.Unique);
        }

        [Test]
        public void DropIndex()
        {
            Factory.TableManager.DropIndex("Name");
            Factory.TableManager.DropIndex("Age");
        }

        [Test]
        public void AlertIndex()
        {
            Factory.TableManager.AlertIndex("Name", "Name", IndexType.Unique);
        }

        [Test]
        public void ShowIndexList()
        {
            var data = Factory.TableManager.ShowIndexList();
            Console.WriteLine(JsonConvert.SerializeObject(data));
        }

        [Test]
        public void GetIndexEntityList()
        {
            var data = Factory.TableManager.GetIndexEntityList();
            Console.WriteLine(JsonConvert.SerializeObject(data));
        }

        [Test]
        public void ShowColumnList()
        {
            var data = Factory.TableManager.ShowColumnList();
            Console.WriteLine(JsonConvert.SerializeObject(data));
        }

        [Test]
        public void GetColumnEntityList()
        {
            var data = Factory.TableManager.GetColumnEntityList();
            Console.WriteLine(JsonConvert.SerializeObject(data));
        }

        [Test]
        public void ReName()
        {
            Factory.TableManager.ReName("1111");
        }

        [Test]
        public void SetComment()
        {
            Factory.TableManager.SetComment("人类表");
        }

        [Test]
        public void SetCharset()
        {
            Factory.TableManager.SetCharset("utf8");
        }

        [Test]
        public void AddColumn()
        {
            Factory.TableManager.AddColumn("NewColumn", typeof(string), 60, "新增字段");
        }

        [Test]
        public void DropColumn()
        {
            Factory.TableManager.DropColumn("NewColumn");
        }

        [Test]
        public void AddColumnAfter()
        {
            Factory.TableManager.AddColumnAfter("NewColumn","IsAdmin", typeof(string), 60, "新增字段");
        }

    }
}
