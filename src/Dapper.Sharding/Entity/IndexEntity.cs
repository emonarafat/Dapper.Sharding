namespace Dapper.Sharding
{
    public class IndexEntity
    {
        public string Name { get; set; }

        public string Columns { get; set; }

        public IndexType Type { get; set; }

        public string StringType { get; set; }
    }
}
