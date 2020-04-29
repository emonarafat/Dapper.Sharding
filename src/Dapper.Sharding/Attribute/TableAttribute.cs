using System;

namespace Dapper.Sharding
{
    [AttributeUsage(AttributeTargets.Class)]
    public class TableAttribute : Attribute
    {
        public string PrimaryKey;

        public bool IsIdentity;

        public string Comment;

        public TableAttribute(string primaryKey, bool isIdentity = true, string comment = null)
        {
            PrimaryKey = primaryKey;
            IsIdentity = isIdentity;
            Comment = comment;
        }

    }
}
