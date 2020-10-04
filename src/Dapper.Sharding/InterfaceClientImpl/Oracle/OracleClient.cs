using System;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;

namespace Dapper.Sharding
{
    internal class OracleClient : IClient
    {

        public OracleClient(DataBaseConfig config) : base(DataBaseType.Oracle, config)
        {
            ConnectionString = ConnectionStringBuilder.BuilderOracleSysdba(config);
        }

        public override string ConnectionString { get; }


        #region protected method

        protected override IDatabase CreateIDatabase(string name)
        {
            return new OracleDatabase(name, this);
        }

        #endregion

        public override void CreateDatabase(string name)
        {
            throw new NotImplementedException();
        }

        public override void DropDatabase(string name)
        {
            throw new NotImplementedException();
        }

        public override bool ExistsDatabase(string name)
        {
            throw new NotImplementedException();
        }

        public override IDbConnection GetConn()
        {
            throw new NotImplementedException();
        }

        public override Task<IDbConnection> GetConnAsync()
        {
            throw new NotImplementedException();
        }

        public override IEnumerable<string> ShowDatabases()
        {
            throw new NotImplementedException();
        }

        public override IEnumerable<string> ShowDatabasesExcludeSystem()
        {
            throw new NotImplementedException();
        }

    }
}
