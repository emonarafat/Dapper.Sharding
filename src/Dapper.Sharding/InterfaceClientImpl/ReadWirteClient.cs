namespace Dapper.Sharding
{
    public class ReadWirteClient
    {
        public ReadWirteClient(IClient writeClient, params IClient[] readClientList)
        {
            WriteClient = writeClient;
            _readClientList = readClientList;
            if (_readClientList.Length > 1)
                _balance = new BalanceRound<IClient>(readClientList);
        }

        private IClient WriteClient { get; }
        private readonly IClient[] _readClientList;
        private BalanceRound<IClient> _balance;

        public IClient GetReadClient()
        {
            if (_readClientList.Length == 1)
                return _readClientList[0];
            return _balance.Get();
        }

        public IClient GetWriteClient()
        {
            return WriteClient;
        }

        public IDatabase GetReadDatabase(string name)
        {
            return GetReadClient().GetDatabase(name);
        }

        public IDatabase GetWriteDataBase(string name)
        {
            return GetWriteClient().GetDatabase(name);
        }

        public ITable<T> GetReadTable<T>(string name, string databaseName)
        {
            return GetReadDatabase(databaseName).GetTable<T>(name);
        }

        public ITable<T> GetWriteTable<T>(string name, string databaseName)
        {
            return GetWriteDataBase(databaseName).GetTable<T>(name);
        }
    }
}
