namespace Dapper.Sharding
{
    public class DataBaseConfig
    {
        public string Server { get; set; }

        public int Port { get; set; }

        public string UserId { get; set; }

        public string Password { get; set; }

        public int MaxPoolSize { get; set; }

        public string CharSet { get; set; }

        public int TimeOut { get; set; }

        public string Oracle_ServiceName { get; set; }

        public string Oracle_SysUserId { get; set; }

        public string Oracle_SysPassword { get; set; }

        public string Oracle_DatabaseDirectory { get; set; }

        public int Oracle_TableSpace_Mb { get; set; }

        public int Oracle_TableSpace_NextMb { get; set; }

    }
}
