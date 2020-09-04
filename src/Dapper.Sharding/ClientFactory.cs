namespace Dapper.Sharding
{
    public class ClientFactory
    {
        public static IClient CreateClient(DataBaseType dbType, string connectionString)
        {
            switch (dbType)
            {
                case DataBaseType.MySql:
                    return new  MySqlClient(connectionString);
            }
            return null;
        }
    }
}
