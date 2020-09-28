using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dapper.Sharding
{
    internal class PostgreClient : IClient
    {
        public PostgreClient(string connectionString) : base(DataBaseType.Postgresql, connectionString)
        {

        }

        #region protected method

        protected override IDatabase CreateIDatabase(string name)
        {
            throw new NotImplementedException();
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
