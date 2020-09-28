using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

namespace Dapper.Sharding
{
    public abstract class IDatabase
    {
        public IDatabase(string name, IClient client)
        {
            Name = name;
            Client = client;
        }

        #region protected method

        protected LockManager Locker { get; } = new LockManager();

        protected ConcurrentDictionary<string, object> TableCache { get; } = new ConcurrentDictionary<string, object>();

        protected abstract ITable<T> GetITable<T>(string name);

        #endregion

        #region public method

        public string Name { get; }

        public IClient Client { get; }

        public void Using(Action<IDbConnection> action)
        {
            using (var conn = GetConn())
            {
                action(conn);
            }
        }

        public void UsingTran(Action<IDbConnection, IDbTransaction> action)
        {
            using (var conn = GetConn())
            {
                using (var tran = conn.BeginTransaction())
                {
                    action(conn, tran);
                }
            }
        }

        public void CreateTable<T>(string name)
        {
            var script = GetTableScript<T>(name);
            using (var conn = GetConn())
            {
                conn.Execute(script);
            }
        }

        public ITable<T> GetTable<T>(string name)
        {
            var lowerName = name.ToLower();
            if (!TableCache.ContainsKey(lowerName))
            {
                lock (Locker.GetObject(lowerName))
                {
                    if (!TableCache.ContainsKey(lowerName))
                    {
                        if (Client.AutoCreateTable)
                        {
                            #region 创建、比对表

                            if (!ExistsTable(name))
                            {
                                CreateTable<T>(name);
                            }
                            else
                            {
                                if (Client.AutoCompareTableColumn)
                                {
                                    var dbColumns = GetTableColumnList(name);
                                    var tableEntity = ClassToTableEntityUtils.Get<T>();
                                    var manager = GetTableManager(name);

                                    foreach (var item in tableEntity.ColumnList)
                                    {
                                        if (!dbColumns.Any(a => a.ToLower().Equals(item.Name.ToLower())))
                                        {
                                            manager.AddColumn(item.Name, item.CsType, item.Length, item.Comment);
                                        }
                                    }

                                    foreach (var item in dbColumns)
                                    {
                                        if (!tableEntity.ColumnList.Any(a => a.Name.ToLower().Equals(item.ToLower())))
                                        {
                                            manager.DropColumn(item);
                                        }
                                    }
                                }
                            }

                            #endregion
                        }
                        TableCache.TryAdd(lowerName, GetTable<T>(name));
                    }
                }
            }
            return (ITable<T>)TableCache[lowerName];
        }

        public void GeneratorClassFile(string savePath, string tableList = "*", string nameSpace = "Model", string Suffix = "Table", bool partialClass = false)
        {
            this.CreateFiles(savePath, tableList, nameSpace, Suffix, partialClass);
        }


        #endregion

        #region abstract method

        public abstract IDbConnection GetConn();

        public abstract Task<IDbConnection> GetConnAsync();

        public abstract void SetCharset(string chartset);

        public abstract void DropTable(string name);

        public abstract void TruncateTable(string name);

        public abstract IEnumerable<string> GetTableList();

        public abstract IEnumerable<string> GetTableColumnList(string name);

        public abstract bool ExistsTable(string name);

        public abstract string GetTableScript<T>(string name);

        public abstract TableEntity GetTableEntityFromDatabase(string name);

        public abstract ITableManager GetTableManager(string name);

        #endregion




    }
}
