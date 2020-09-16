namespace Dapper.Sharding
{
    public class ShardingFactory
    {
        public static IClient CreateClient(DataBaseType dbType, string connectionString)
        {
            switch (dbType)
            {
                case DataBaseType.MySql:
                    return new MySqlClient(connectionString);
                case DataBaseType.SqlServer2008:
                    return new SqlServerClient(connectionString, DataBaseType.SqlServer2008);
                case DataBaseType.SqlServer2012:
                    return new SqlServerClient(connectionString, DataBaseType.SqlServer2012);
                case DataBaseType.Sqlite:
                    return new SQLiteClient(connectionString);
                case DataBaseType.Postgresql:
                    return new PostgreClient(connectionString);
                case DataBaseType.Oracel:
                    return new OracleClient(connectionString);
            }
            return null;
        }
    }
}
