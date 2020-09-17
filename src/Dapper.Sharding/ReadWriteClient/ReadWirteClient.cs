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

        private readonly IClient[] _readClientList;
        private BalanceRound<IClient> _balance;

        public IClient WriteClient { get; }

        public IClient ReadClient
        {
            get
            {
                if (_readClientList.Length == 1)
                    return _readClientList[0];
                return _balance.Get();
            }
        }


    }
}
