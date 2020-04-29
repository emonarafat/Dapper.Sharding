namespace Dapper.Sharding
{
    public class BalanceRound<T>
    {
        BalanceRoundBase balanceRoundBase;
        readonly T[] _objs;

        public BalanceRound(params T[] objs)
        {
            balanceRoundBase = new BalanceRoundBase(objs.Length);
            _objs = objs;
        }

        public T Get()
        {
            return _objs[balanceRoundBase.Get()];
        }

        public T GetAsc()
        {
            return _objs[balanceRoundBase.GetAsc()];
        }

    }
}
