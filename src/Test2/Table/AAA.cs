using Dapper.Sharding;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Test2
{
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

        [Ignore]
        public List<dynamic> _NextList2 { get; set; }
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

        [Ignore]
        public List<dynamic> _PrevList2 { get; set; }
    }
}
