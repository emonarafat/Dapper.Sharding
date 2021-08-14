using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Dapper.Sharding
{
    public abstract class ITableManager
    {
        public ITableManager(string name, IDatabase database)
        {
            Name = name;
            DataBase = database;
        }


        #region prototype

        public string Name { get; }

        public IDatabase DataBase { get; }

        #endregion

        public abstract void CreateIndex(string name, string columns, IndexType indexType);

        public abstract Task CreateIndexAsync(string name, string columns, IndexType indexType);

        public abstract void DropIndex(string name);

        public abstract Task DropIndexAsync(string name);

        public abstract void AddColumn(string name, Type t, double length = 0, string comment = null);

        public abstract Task AddColumnAsync(string name, Type t, double length = 0, string comment = null);

        public abstract void DropColumn(string name);

        public abstract Task DropColumnAsync(string name);

        public abstract void ModifyColumn(string name, Type t, double length = 0, string comment = null);

        public abstract Task ModifyColumnAsync(string name, Type t, double length = 0, string comment = null);

        public abstract List<IndexEntity> GetIndexEntityList();

        public abstract Task<List<IndexEntity>> GetIndexEntityListAsync();

        public abstract List<ColumnEntity> GetColumnEntityList(TableEntity tb = null);

        public abstract Task<List<ColumnEntity>> GetColumnEntityListAsync(TableEntity tb = null);

    }
}
