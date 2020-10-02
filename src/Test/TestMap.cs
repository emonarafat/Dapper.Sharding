using Dapper.Sharding;
using Newtonsoft.Json;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.AccessControl;
using System.Text;
using System.Threading.Tasks;

namespace Test
{

    public class TestMap
    {
        [Test]
        public void Map()
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

            var table1 = Factory.Client.GetDatabase("test").GetTable<MapAuthor>("MapAuthor");
            table1.Merge(list1);
            var table2 = Factory.Client.GetDatabase("test").GetTable<MapBook>("MapBook");
            table2.Merge(list2);

            //one to many
            var data1 = table1.GetAll();
            data1.MapTableOneToMany("Id", "_BookList", table2, "AuthorId");
            Console.WriteLine(JsonConvert.SerializeObject(data1));

            //one to one
            var data2 = table2.GetAll();
            data2.MapTableOneToOne("AuthorId", "_Author", table1, "Id");
            Console.WriteLine(JsonConvert.SerializeObject(data2));

            var list3 = new MapAuthor { Id = 1, Name = "李白" };
            Console.WriteLine(JsonConvert.SerializeObject(list3));
        }

        [Test]
        public void MapCenter()
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

            var table1 = Factory.Client.GetDatabase("test").GetTable<Map_Prev>("Map_Prev");
            table1.Merge(list1);
            var centerTable = Factory.Client.GetDatabase("test").GetTable<Map_Center>("Map_Center");
            centerTable.Merge(centerList);
            var table3 = Factory.Client.GetDatabase("test").GetTable<Map_Next>("Map_Next");
            table3.Merge(list3);

            var data1 = table1.GetAll();
            data1.MapTableManyToMany("Id", "_NextList", centerTable, "FirstId", "NextId", table3, "Id");
            Console.WriteLine(JsonConvert.SerializeObject(data1));


            var data2 = table3.GetAll();
            data2.MapTableManyToMany("Id", "_PrevList", centerTable, "NextId", "FirstId", table1, "Id");
            Console.WriteLine(JsonConvert.SerializeObject(data2));
        }
    }


    [Table("Id", false)]
    public class MapAuthor
    {
        public long Id { get; set; }

        public string Name { get; set; }

        [Dapper.Sharding.Ignore]
        public List<MapBook> _BookList { get; set; }

    }

    [Table("Id", false)]
    public class MapBook
    {
        public long Id { get; set; }

        public long AuthorId { get; set; }

        public string Name { get; set; }

        [Dapper.Sharding.Ignore]
        public MapAuthor _Author { get; set; }
    }

    [Table("Id", false)]
    public class Map_Prev
    {
        public int Id { get; set; }

        public string Name { get; set; }

        [Dapper.Sharding.Ignore]
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

        [Dapper.Sharding.Ignore]
        public List<Map_Prev> _PrevList { get; set; }
    }

}
