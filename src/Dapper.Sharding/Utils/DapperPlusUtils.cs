using System.Collections.Generic;
using Z.Dapper.Plus;

namespace Dapper.Sharding
{
    public class DapperPlusUtils
    {
        private static object _dapperPlusLocker = new object();

        private static HashSet<string> _dapperPlusDict = new HashSet<string>();

        public static void Map<T>(string tableName) where T : class
        {
            if (!_dapperPlusDict.Contains(tableName))
            {
                lock (_dapperPlusLocker)
                {
                    if (!_dapperPlusDict.Contains(tableName))
                    {
                        var sqlField = new SqlFieldEntity(ClassToTableEntityUtils.Get<T>(), "", "", "@");
                        DapperPlusEntityMapper<T> map;
                        if (sqlField.IsIdentity)
                        {
                            map = DapperPlusManager.Entity<T>(tableName).Identity(sqlField.PrimaryKey).Table(tableName);
                        }
                        else
                        {
                            map = DapperPlusManager.Entity<T>(tableName).Key(sqlField.PrimaryKey).Table(tableName);
                        }
                        //去除忽略
                        foreach (var item in sqlField.IgnoreFieldList)
                        {
                            map.Ignore(item);
                        }
                        _dapperPlusDict.Add(tableName);
                    }
                }
            }
        }

    }
}
