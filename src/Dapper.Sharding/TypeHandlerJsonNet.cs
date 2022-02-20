namespace Dapper.Sharding
{
    public class TypeHandlerJsonNet
    {
        public static void Add<T>()
        {
            SqlMapper.AddTypeHandler(new JsonNetTypeHandler<T>());
        }
    }
}
