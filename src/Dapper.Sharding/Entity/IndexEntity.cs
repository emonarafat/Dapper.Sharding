namespace Dapper.Sharding
{
    public class IndexEntity
    {
        public string Name { get; set; }

        public string Columns { get; set; }

        public IndexType Type { get; set; }

        public string StringType 
        { 
            get 
            {
                switch (Type)
                {
                    case IndexType.PrimaryKey:return "IndexType.PrimaryKey";
                    case IndexType.Unique: return "IndexType.Unique";
                    case IndexType.Normal: return "IndexType.Normal";
                    case IndexType.FullText: return "IndexType.FullText";
                    case IndexType.Spatial: return "IndexType.Spatial";
                }
                return null;
            } 
        }
    }
}
