using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dapper.Sharding
{
    internal class OracleDatabase : IDatabase
    {
        public OracleDatabase(string name, OracleClient client) : base(name, client)
        {
            ConnectionString = ConnectionStringBuilder.BuilderOracle(client.Config, name);
        }

        public override string ConnectionString { get; }

        public override void DropTable(string name)
        {
            throw new NotImplementedException();
        }

        public override bool ExistsTable(string name)
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

        public override IEnumerable<string> GetTableColumnList(string name)
        {
            throw new NotImplementedException();
        }

        public override TableEntity GetTableEntityFromDatabase(string name)
        {
            throw new NotImplementedException();
        }

        public override IEnumerable<string> GetTableList()
        {
            throw new NotImplementedException();
        }

        public override ITableManager GetTableManager(string name)
        {
            return new OracleTableManager(name, this);
        }

        public override string GetTableScript<T>(string name)
        {
            throw new NotImplementedException();
        }

        public override void SetCharset(string chartset)
        {
            throw new NotImplementedException();
        }

        public override void TruncateTable(string name)
        {
            throw new NotImplementedException();
        }

        protected override ITable<T> CreateITable<T>(string name)
        {
            return new OracleTable<T>(name, this);
        }
    }
}
