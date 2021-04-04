using Dapper.Sharding;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Test2
{
    [Table("Id", false)]
    public class AA
    {
        public int Id { get; set; }

        public string BId { get; set; }

        [Ignore]
        public BB _BB { get; set; }

        [Ignore]
        public dynamic _BB2 { get; set; }

    }

    [Table("Id", false)]
    public class BB
    {
        public string Id { get; set; }

        public string Name { get; set; }

        public int Age { get; set; }

        public int Sex { get; set; }
    }
}
