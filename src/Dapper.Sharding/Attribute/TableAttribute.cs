using System;

namespace Dapper.Sharding
{
    [AttributeUsage(AttributeTargets.Class)]
    public class TableAttribute : Attribute
    {
        public string PrimaryKey;

        public bool IsIdentity;

        public string Comment;

        public string Engine;

        public string Cluster;

        public TableAttribute(string primaryKey, bool isIdentity = true, string comment = null, string engine = null, string cluster = null)
        {
            PrimaryKey = primaryKey;
            IsIdentity = isIdentity;
            Comment = comment;
            Engine = engine;
            Cluster = cluster;
        }

    }
}
