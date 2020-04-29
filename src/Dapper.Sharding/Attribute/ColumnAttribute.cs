using System;

namespace Dapper.Sharding
{
    [AttributeUsage(AttributeTargets.Property)]
    public class ColumnAttribute : Attribute
    {
        /// <summary>
        /// 字段长度
        /// </summary>
        public double Length;

        /// <summary>
        /// 列注释
        /// </summary>
        public string Comment;

        public ColumnAttribute(double length = 0, string comment = null)
        {
            Length = length;
            Comment = comment;
        }


    }
}
