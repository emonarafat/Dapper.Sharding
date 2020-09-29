using System;
using System.Collections.Concurrent;

namespace Dapper.Sharding
{
    internal class SqlFieldCacheUtils
    {
        #region Mysql

        private static readonly ConcurrentDictionary<RuntimeTypeHandle, SqlFieldEntity> MySqlDict = new ConcurrentDictionary<RuntimeTypeHandle, SqlFieldEntity>();

        private static readonly object _mysqlLocker = new object();

        public static SqlFieldEntity GetMySqlFieldEntity<T>()
        {
            var typeHandle = typeof(T).TypeHandle;
            if (!MySqlDict.Keys.Contains(typeHandle))
            {
                lock (_mysqlLocker)
                {
                    if (!MySqlDict.Keys.Contains(typeHandle))
                    {
                        MySqlDict[typeHandle] = new SqlFieldEntity(ClassToTableEntityUtils.Get<T>(), "`", "`", "@");
                    }
                }
            }

            return MySqlDict[typeHandle];
        }

        #endregion

        #region SqlServer

        private static readonly ConcurrentDictionary<RuntimeTypeHandle, SqlFieldEntity> SqlServerDict = new ConcurrentDictionary<RuntimeTypeHandle, SqlFieldEntity>();

        private static readonly object _sqlserverLocker = new object();

        public static SqlFieldEntity GetSqlServerFieldEntity<T>()
        {
            var typeHandle = typeof(T).TypeHandle;
            if (!SqlServerDict.Keys.Contains(typeHandle))
            {
                lock (_sqlserverLocker)
                {
                    if (!SqlServerDict.Keys.Contains(typeHandle))
                    {
                        SqlServerDict[typeHandle] = new SqlFieldEntity(ClassToTableEntityUtils.Get<T>(), "[", "]", "@");
                    }
                }
            }

            return SqlServerDict[typeHandle];
        }

        #endregion

        #region Sqlite

        private static readonly ConcurrentDictionary<RuntimeTypeHandle, SqlFieldEntity> SqliteDict = new ConcurrentDictionary<RuntimeTypeHandle, SqlFieldEntity>();

        private static readonly object _sqliteLocker = new object();

        public static SqlFieldEntity GetSqliteFieldEntity<T>()
        {
            var typeHandle = typeof(T).TypeHandle;
            if (!SqliteDict.Keys.Contains(typeHandle))
            {
                lock (_sqliteLocker)
                {
                    if (!SqliteDict.Keys.Contains(typeHandle))
                    {
                        SqliteDict[typeHandle] = new SqlFieldEntity(ClassToTableEntityUtils.Get<T>(), "[", "]", "@");
                    }
                }
            }

            return SqliteDict[typeHandle];
        }

        #endregion

        #region Postgresql

        private static readonly ConcurrentDictionary<RuntimeTypeHandle, SqlFieldEntity> PostgreDict = new ConcurrentDictionary<RuntimeTypeHandle, SqlFieldEntity>();

        private static readonly object _postgreLocker = new object();

        public static SqlFieldEntity GetPostgreFieldEntity<T>()
        {
            var typeHandle = typeof(T).TypeHandle;
            if (!PostgreDict.Keys.Contains(typeHandle))
            {
                lock (_postgreLocker)
                {
                    if (!PostgreDict.Keys.Contains(typeHandle))
                    {
                        //PostgreDict[typeHandle] = new SqlFieldEntity(ClassToTableEntityUtils.Get<T>(), "\"", "\"", "@");
	        PostgreDict[typeHandle] = new SqlFieldEntity(ClassToTableEntityUtils.Get<T>(), "", "", "@");
                    }
                }
            }

            return PostgreDict[typeHandle];
        }

        #endregion

        #region Oracle

        private static readonly ConcurrentDictionary<RuntimeTypeHandle, SqlFieldEntity> OracleDict = new ConcurrentDictionary<RuntimeTypeHandle, SqlFieldEntity>();

        private static readonly object _oracleLocker = new object();

        public static SqlFieldEntity GetOracleFieldEntity<T>()
        {
            var typeHandle = typeof(T).TypeHandle;
            if (!OracleDict.Keys.Contains(typeHandle))
            {
                lock (_oracleLocker)
                {
                    if (!OracleDict.Keys.Contains(typeHandle))
                    {
                        //OracleDict[typeHandle] = new SqlFieldEntity(ClassToTableEntityUtils.Get<T>(), "\"", "\"", ":");
                        OracleDict[typeHandle] = new SqlFieldEntity(ClassToTableEntityUtils.Get<T>(), "", "", ":");
                    }
                }
            }
            return OracleDict[typeHandle];
        }

        #endregion


    }
}
