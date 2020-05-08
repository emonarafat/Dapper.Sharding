namespace Dapper.Sharding
{
    public class DapperFactory
    {
        public static IClient CreateMySqlClient(string connectionString)
        {
            return new MySqlClient(connectionString);
        }

        //public static IDapperClient CreateSqlServerClient(string connectionString)
        //{
        //    return new SqlServerClient(connectionString);
        //}

    }
}
