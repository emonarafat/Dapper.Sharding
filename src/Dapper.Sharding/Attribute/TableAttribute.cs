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

        public TableAttribute(string primaryKey, bool isIdentity = true, string comment = null, string engine = null)
        {
            PrimaryKey = primaryKey;
            IsIdentity = isIdentity;
            Comment = comment;
            Engine = engine;
        }

    }
}
