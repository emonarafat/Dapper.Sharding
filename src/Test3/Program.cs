using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Unicode;
using Dapper.Sharding;
using System.Linq;
using System.Linq.Dynamic.Core;
using Dapper;
using System.Dynamic;
using System.Linq.Dynamic.Core.CustomTypeProviders;

namespace Test2
{
    class Program
    {
        static void Main(string[] args)
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

            var table1 = Factory.Client.GetDatabase("test").GetTable<MapAuthor>("map_author");
            table1.Merge(list1);
            var table2 = Factory.Client.GetDatabase("test").GetTable<MapBook>("map_book");
            table2.Merge(list2);

            var options = new JsonSerializerOptions();
            options.Encoder = System.Text.Encodings.Web.JavaScriptEncoder.Create(UnicodeRanges.All);

            //one to one
            var data2 = table2.GetAll();
            //data2.MapTableOneToOne("AuthorId", "_Author", table1, "Id");
            //Console.WriteLine(JsonSerializer.Serialize(data2, options));

            data2.MapTableOneToOneDynamic("AuthorId", "_Author2", table1, "Id", "Id,Name".AsPgsqlField());
            Console.WriteLine(JsonSerializer.Serialize(data2, options));

            //one to many
            var data1 = table1.GetAll();
            //data1.MapTableOneToMany("Id", "_BookList", table2, "AuthorId");
            //Console.WriteLine(JsonSerializer.Serialize(data1, options));

            data1.MapTableOneToManyDynamic("Id", "_BookList2", table2, "AuthorId", "Id,Name,AuthorId".AsPgsqlField());
            Console.WriteLine(JsonSerializer.Serialize(data1, options));


            var list11 = new List<AAA>
            {
                new AAA{ Id=1,BId=null },
                new AAA{ Id=2,BId="" },
                new AAA{ Id=3,BId="1" },
                new AAA{ Id=4,BId="2" }
            };

            var list22 = new List<BBB>
            {
                new BBB { Id = "", Name = "哈哈1" },
                new BBB { Id = "1", Name = "哈哈2" },
                new BBB{ Id = "2", Name = "哈哈3" }
            };

            var table11 = Factory.Client.GetDatabase("test").GetTable<AAA>("aaa");
            table11.Merge(list11);
            var table22 = Factory.Client.GetDatabase("test").GetTable<BBB>("bbb");
            table22.Merge(list22);

            var data11 = table11.GetAll();
            data11.MapTableOneToOneDynamic("BId", "_BB", table22, "Id", "Id,Name".AsPgsqlField());
            Console.WriteLine(JsonSerializer.Serialize(data11,options));


            Console.ReadKey();
        }
    }

    #region MyRegion

    [Table("Id", false)]
    public class MapAuthor
    {
        public long Id { get; set; }

        public string Name { get; set; }

        [Ignore]
        public List<MapBook> _BookList { get; set; }

        [Ignore]
        public List<dynamic> _BookList2 { get; set; }

    }

    [Table("Id", false)]
    public class MapBook
    {
        public long Id { get; set; }

        public long AuthorId { get; set; }

        public string Name { get; set; }

        [Ignore]
        public MapAuthor _Author { get; set; }

        [Ignore]
        public dynamic _Author2 { get; set; }
    }

    [Table("Id", false)]
    public class Map_Prev
    {
        public int Id { get; set; }

        public string Name { get; set; }

        [Ignore]
        public List<Map_Next> _NextList { get; set; }
    }

    [Table("Id", false)]
    public class Map_Center
    {
        public int Id { get; set; }

        public int FirstId { get; set; }

        public int NextId { get; set; }
    }

    [Table("Id", false)]
    public class Map_Next
    {
        public int Id { get; set; }

        public string Name { get; set; }

        [Ignore]
        public List<Map_Prev> _PrevList { get; set; }
    }

    [Table("Id", false)]
    public class AAA
    {
        public int Id { get; set; }

        public string BId { get; set; }

        [Ignore]
        public dynamic _BB { get; set; }

    }

    [Table("Id", false)]
    public class BBB
    {
        public string Id { get; set; }

        public string Name { get; set; }

    }

    #endregion
}
