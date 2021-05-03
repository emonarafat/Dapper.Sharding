using Dapper.Sharding;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Test2
{
    [Table("Id", true)]
    [Index("a", "a", IndexType.Gist)]
    [Index("b", "b", IndexType.JsonbGin)]
    [Index("b2", "b", IndexType.JsonbGinPath)]
    [Index("c", "(c->>'name')", IndexType.JsonBtree)]
    public class CCC
    {
        public int Id { get; set; }

        [Column(-20)]
        public string a { get; set; }

        [Column(-10)]
        public string b { get; set; }

        [Column(-11)]
        public string c { get; set; }

        public string d { get; set; }

        [Column(-1)]
        public string e { get; set; }

        [Column(65)]
        public string f { get; set; }
    }
}
