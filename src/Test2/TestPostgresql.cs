using Dapper.Sharding;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.Unicode;
using System.Threading.Tasks;
namespace Test2
{
    public class TestPostgresql
    {

        [Test]
        public void AA()
        {
            var list1 = new List<MapAuthor>
            {
                new MapAuthor{ Id=1,Name="李白" },
                new MapAuthor{ Id=2,Name="杜甫" },
                new MapAuthor{ Id=3,Name="涛神" },
                new MapAuthor{ Id=4,Name="吕布" }
            };

            var list2 = new List<MapBook>
            {
                new MapBook{ Id=1,AuthorId=1,Name="苹果" },
                new MapBook{ Id=2,AuthorId=2,Name="橘子" },
                new MapBook{ Id=3,AuthorId=1,Name="香蕉" },
                new MapBook{ Id=4,AuthorId=2,Name="菠萝" },
                new MapBook{ Id=5,AuthorId=1,Name="苹哈密瓜" },
                new MapBook{ Id=6,AuthorId=5,Name="西瓜" }
            };

            var table1 = DbHelper.Client.GetDatabase("test").GetTable<MapAuthor>("map_author");
            table1.Merge(list1);
            var table2 = DbHelper.Client.GetDatabase("test").GetTable<MapBook>("map_book");
            table2.Merge(list2);

            var options = new JsonSerializerOptions();
            options.Encoder = System.Text.Encodings.Web.JavaScriptEncoder.Create(UnicodeRanges.All);

            //one to many
            var data1 = table1.GetAll();
            data1.TableOneToMany("Id", "_BookList", table2, "AuthorId");
            Console.WriteLine(JsonSerializer.Serialize(data1, options));

            //one to one
            var data2 = table2.GetAll();
            data2.TableOneToOne("AuthorId", "_Author", table1, "Id");
            Console.WriteLine(JsonSerializer.Serialize(data2, options));
        }

        [Test]
        public void AA2()
        {
            var list1 = new List<MapAuthor>
            {
                new MapAuthor{ Id=1,Name="李白" },
                new MapAuthor{ Id=2,Name="杜甫" },
                new MapAuthor{ Id=3,Name="涛神" },
                new MapAuthor{ Id=4,Name="吕布" }
            };

            var list2 = new List<MapBook>
            {
                new MapBook{ Id=1,AuthorId=1,Name="苹果" },
                new MapBook{ Id=2,AuthorId=2,Name="橘子" },
                new MapBook{ Id=3,AuthorId=1,Name="香蕉" },
                new MapBook{ Id=4,AuthorId=2,Name="菠萝" },
                new MapBook{ Id=5,AuthorId=1,Name="苹哈密瓜" },
                new MapBook{ Id=6,AuthorId=5,Name="西瓜" }
            };

            var table1 = DbHelper.Client.GetDatabase("test").GetTable<MapAuthor>("map_author");
            table1.Merge(list1);
            var table2 = DbHelper.Client.GetDatabase("test").GetTable<MapBook>("map_book");
            table2.Merge(list2);

            var options = new JsonSerializerOptions();
            options.Encoder = System.Text.Encodings.Web.JavaScriptEncoder.Create(UnicodeRanges.All);

            //one to many
            var data1 = table1.GetAll();
            data1.TableOneToManyDynamic("Id", "_BookList2", table2, "AuthorId", "Id,Name,AuthorId");
            Console.WriteLine(JsonSerializer.Serialize(data1, options));

            //one to one
            var data2 = table2.GetAll();
            data2.TableOneToOneDynamic("AuthorId", "_Author2", table1, "Id", "Id,Name");
            Console.WriteLine(JsonSerializer.Serialize(data2, options));
        }

        [Test]
        public void BB()
        {
            var list1 = new List<AA>
            {
                new AA{ Id=1,BId=null },
                new AA{ Id=2,BId="" },
                new AA{ Id=3,BId="1" },
                new AA{ Id=4,BId="2" }
            };

            var list2 = new List<BB>
            {
                new BB { Id = "", Name = "哈哈1" },
                new BB { Id = "1", Name = "哈哈2" },
                new BB { Id = "2", Name = "哈哈3" }
            };

            var options = new JsonSerializerOptions();
            options.Encoder = System.Text.Encodings.Web.JavaScriptEncoder.Create(UnicodeRanges.All);

            var table1 = DbHelper.Client.GetDatabase("test").GetTable<AA>("aa");
            table1.Merge(list1);
            var table2 = DbHelper.Client.GetDatabase("test").GetTable<BB>("bb");
            table2.Merge(list2);

            var data1 = table1.GetAll();
            data1.TableOneToOne("BId", "_BB", table2, "Id");
            Console.WriteLine(JsonSerializer.Serialize(data1, options));
        }

        [Test]
        public void BB2()
        {
            var list1 = new List<AA>
            {
                new AA{ Id=1,BId=null },
                new AA{ Id=2,BId="" },
                new AA{ Id=3,BId="1" },
                new AA{ Id=4,BId="2" }
            };

            var list2 = new List<BB>
            {
                new BB { Id = "", Name = "哈哈1" },
                new BB { Id = "1", Name = "哈哈2" },
                new BB { Id = "2", Name = "哈哈3" }
            };

            var options = new JsonSerializerOptions();
            options.Encoder = System.Text.Encodings.Web.JavaScriptEncoder.Create(UnicodeRanges.All);

            var table1 = DbHelper.Client.GetDatabase("test").GetTable<AA>("aa");
            table1.Merge(list1);
            var table2 = DbHelper.Client.GetDatabase("test").GetTable<BB>("bb");
            table2.Merge(list2);

            var data1 = table1.GetAll();
            data1.TableOneToOneDynamic("BId", "_BB2", table2, "Id", "Id,Name");
            Console.WriteLine(JsonSerializer.Serialize(data1, options));
        }

        [Test]
        public void CC()
        {
            var list1 = new List<Map_Prev>
            {
                new Map_Prev{ Id=1,Name="李白" },
                new Map_Prev{ Id=2,Name="杜甫" },
                new Map_Prev{ Id=3,Name="涛神" },
                new Map_Prev{ Id=4,Name="吕布" }
            };

            var centerList = new List<Map_Center>
            {
                new Map_Center{ Id=1, FirstId = 1, NextId = 1 },
                new Map_Center{ Id=2, FirstId = 1, NextId = 2 },
                new Map_Center{ Id=3, FirstId = 1, NextId = 3 },
                new Map_Center{ Id=4, FirstId = 3, NextId = 5 },
                new Map_Center{ Id=5, FirstId = 2, NextId = 6 },
                new Map_Center{ Id=6, FirstId = 2, NextId = 1 },
                new Map_Center{ Id=7, FirstId = 1, NextId = 4 },
                new Map_Center{ Id=8, FirstId = 2, NextId = 3 },
            };

            var list3 = new List<Map_Next>
            {
                new Map_Next{ Id=1,Name="苹果" },
                new Map_Next{ Id=2,Name="橘子" },
                new Map_Next{ Id=3,Name="香蕉" },
                new Map_Next{ Id=4,Name="菠萝" },
                new Map_Next{ Id=5,Name="苹哈密瓜" },
                new Map_Next{ Id=6,Name="西瓜" }
            };

            var table1 = DbHelper.Client.GetDatabase("test").GetTable<Map_Prev>("map_prev");
            table1.Merge(list1);
            var centerTable = DbHelper.Client.GetDatabase("test").GetTable<Map_Center>("map_center");
            centerTable.Merge(centerList);
            var table3 = DbHelper.Client.GetDatabase("test").GetTable<Map_Next>("map_next");
            table3.Merge(list3);

            var options = new JsonSerializerOptions();
            options.Encoder = System.Text.Encodings.Web.JavaScriptEncoder.Create(UnicodeRanges.All);

            var data1 = table1.GetAll();
            data1.TableCenterToMany("Id", "_NextList", centerTable, "FirstId", "NextId", table3, "Id");
            data1.TableCenterToMany("Id", "_NextList", centerTable, "FirstId", "NextId", table3, "Id", null, "AND Id>5");
            Console.WriteLine(JsonSerializer.Serialize(data1, options));


            var data2 = table3.GetAll();
            data2.TableCenterToMany("Id", "_PrevList", centerTable, "NextId", "FirstId", table1, "Id");
            Console.WriteLine(JsonSerializer.Serialize(data2, options));
        }

        [Test]
        public void CC2()
        {
            var list1 = new List<Map_Prev>
            {
                new Map_Prev{ Id=1,Name="李白" },
                new Map_Prev{ Id=2,Name="杜甫" },
                new Map_Prev{ Id=3,Name="涛神" },
                new Map_Prev{ Id=4,Name="吕布" }
            };

            var centerList = new List<Map_Center>
            {
                new Map_Center{ Id=1, FirstId = 1, NextId = 1 },
                new Map_Center{ Id=2, FirstId = 1, NextId = 2 },
                new Map_Center{ Id=3, FirstId = 1, NextId = 3 },
                new Map_Center{ Id=4, FirstId = 3, NextId = 5 },
                new Map_Center{ Id=5, FirstId = 2, NextId = 6 },
                new Map_Center{ Id=6, FirstId = 2, NextId = 1 },
                new Map_Center{ Id=7, FirstId = 1, NextId = 4 },
                new Map_Center{ Id=8, FirstId = 2, NextId = 3 },
            };

            var list3 = new List<Map_Next>
            {
                new Map_Next{ Id=1,Name="苹果" },
                new Map_Next{ Id=2,Name="橘子" },
                new Map_Next{ Id=3,Name="香蕉" },
                new Map_Next{ Id=4,Name="菠萝" },
                new Map_Next{ Id=5,Name="苹哈密瓜" },
                new Map_Next{ Id=6,Name="西瓜" }
            };

            var table1 = DbHelper.Client.GetDatabase("test").GetTable<Map_Prev>("map_prev");
            table1.Merge(list1);
            var centerTable = DbHelper.Client.GetDatabase("test").GetTable<Map_Center>("map_center");
            centerTable.Merge(centerList);
            var table3 = DbHelper.Client.GetDatabase("test").GetTable<Map_Next>("map_next");
            table3.Merge(list3);

            var options = new JsonSerializerOptions();
            options.Encoder = System.Text.Encodings.Web.JavaScriptEncoder.Create(UnicodeRanges.All);

            var data1 = table1.GetAll();
            data1.TableCenterToManyDynamic("Id", "_NextList2", centerTable, "FirstId", "NextId", table3, "Id");
            data1.TableCenterToManyDynamic("Id", "_NextList2", centerTable, "FirstId", "NextId", table3, "Id", null, "AND Id>5");
            Console.WriteLine(JsonSerializer.Serialize(data1, options));

            var data2 = table3.GetAll();
            data2.TableCenterToManyDynamic("Id", "_PrevList2", centerTable, "NextId", "FirstId", table1, "Id");

            Console.WriteLine(JsonSerializer.Serialize(data2, options));
        }

        [Test]
        public void Gist()
        {
            var table1 = DbHelper.Client.GetDatabase("testgist", true).GetTable<CCC>("aa");
            var model = new CCC
            {
                a = "POINT(0 0)",
                aa = "POINT(0 0)",
                b = "[]",
                c = "{}"
            };

            table1.Insert(model);
            table1.Insert(new List<CCC> { model, model });
        }
    }
}
