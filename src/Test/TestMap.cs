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

    public class TestMap
    {
        [Test]
        public void Map()
        {
            var list1 = new List<MapAuthor>
            {
                new MapAuthor{ Id=1,Name="李白" },
                new MapAuthor{ Id=2,Name="杜甫" },
                new MapAuthor{ Id=3,Name="涛神" }
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

}
