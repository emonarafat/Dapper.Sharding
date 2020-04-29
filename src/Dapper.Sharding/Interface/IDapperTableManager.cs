using System;
using System.Collections.Generic;

namespace Dapper.Sharding
{
    public interface IDapperTableManager
    {
        string Name { get; }

        IDapperDatabase DataBase { get; }

        void CreateIndex(string name, string columns, IndexType indexType);

        void DropIndex(string name);

        void AlertIndex(string name, string columns, IndexType indexType);

        IEnumerable<dynamic> GetIndexs();

        List<IndexEntity> GetIndexEntitys();

        IEnumerable<dynamic> GetColumns();

        List<ColumnEntity> GetColumnEntitys();

        void Rename(string name);

        void SetComment(string comment);

        void SetCharset(string name);

        void AddColumn(string name, Type t, double length = 0, string comment = null);

        void DropColumn(string name);

        void AddColumnAfter(string name, string afterName, Type t, double length = 0, string comment = null);

        void AddColumnFirst(string name, Type t, double length = 0, string comment = null);

        void ModifyColumn(string name, Type t, double length = 0, string comment = null);

        void ModifyColumnFirst(string name, Type t, double length = 0, string comment = null);

        void ModifyColumnAfter(string name, string afterName, Type t, double length = 0, string comment = null);

        void ModifyColumnName(string oldName, string newName, Type t, double length = 0, string comment = null);
    }
}
