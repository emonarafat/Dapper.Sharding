using System;
using System.Collections.Generic;
using System.Data;

namespace Dapper.Sharding
{
    public abstract class ITableManager
    {
        public ITableManager(string name, IDatabase database, DapperEntity dpEntity)
        {
            Name = name;
            DataBase = database;
            DpEntity = dpEntity;
        }


        #region prototype

        public string Name { get; }

        public IDatabase DataBase { get; }

        public DapperEntity DpEntity { get; }

        #endregion

        public abstract ITableManager CreateTranManager(IDbConnection conn, IDbTransaction tran, int? commandTimeout = null);

        public abstract void CreateIndex(string name, string columns, IndexType indexType);

        public abstract void DropIndex(string name);

        public abstract void AlertIndex(string name, string columns, IndexType indexType);

        public abstract List<IndexEntity> GetIndexEntityList();

        public abstract List<ColumnEntity> GetColumnEntityList();

        public abstract void ReName(string name);

        public abstract void SetComment(string comment);

        public abstract void SetCharset(string name);

        public abstract void AddColumn(string name, Type t, double length = 0, string comment = null);

        public abstract void DropColumn(string name);

        public abstract void AddColumnAfter(string name, string afterName, Type t, double length = 0, string comment = null);

        public abstract void AddColumnFirst(string name, Type t, double length = 0, string comment = null);

        public abstract void ModifyColumn(string name, Type t, double length = 0, string comment = null);

        public abstract void ModifyColumnFirst(string name, Type t, double length = 0, string comment = null);

        public abstract void ModifyColumnAfter(string name, string afterName, Type t, double length = 0, string comment = null);

        public abstract void ModifyColumnName(string oldName, string newName, Type t, double length = 0, string comment = null);
    }
}
