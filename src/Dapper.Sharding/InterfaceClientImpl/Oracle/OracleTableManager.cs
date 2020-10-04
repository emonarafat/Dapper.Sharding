using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dapper.Sharding
{
    internal class OracleTableManager : ITableManager
    {
        public OracleTableManager(string name, IDatabase database, IDbConnection conn = null, IDbTransaction tran = null, int? commandTimeout = null) : base(name, database, new DapperEntity(name, database, conn, tran, commandTimeout))
        {

        }

        public override ITableManager CreateTranManager(IDbConnection conn, IDbTransaction tran, int? commandTimeout = null)
        {
            throw new NotImplementedException();
        }
        public override void CreateIndex(string name, string columns, IndexType indexType)
        {
            throw new NotImplementedException();
        }


        public override void DropIndex(string name)
        {
            throw new NotImplementedException();
        }

        public override void AlertIndex(string name, string columns, IndexType indexType)
        {
            throw new NotImplementedException();
        }

        public override List<IndexEntity> GetIndexEntityList()
        {
            throw new NotImplementedException();
        }


        public override List<ColumnEntity> GetColumnEntityList()
        {
            throw new NotImplementedException();
        }

        public override void AddColumn(string name, Type t, double length = 0, string comment = null)
        {
            throw new NotImplementedException();
        }

        public override void AddColumnAfter(string name, string afterName, Type t, double length = 0, string comment = null)
        {
            throw new NotImplementedException();
        }

        public override void AddColumnFirst(string name, Type t, double length = 0, string comment = null)
        {
            throw new NotImplementedException();
        }

        public override void DropColumn(string name)
        {
            throw new NotImplementedException();
        }


        public override void ModifyColumn(string name, Type t, double length = 0, string comment = null)
        {
            throw new NotImplementedException();
        }

        public override void ModifyColumnAfter(string name, string afterName, Type t, double length = 0, string comment = null)
        {
            throw new NotImplementedException();
        }

        public override void ModifyColumnFirst(string name, Type t, double length = 0, string comment = null)
        {
            throw new NotImplementedException();
        }

        public override void ModifyColumnName(string oldName, string newName, Type t, double length = 0, string comment = null)
        {
            throw new NotImplementedException();
        }

        public override void ReName(string name)
        {
            throw new NotImplementedException();
        }

        public override void SetCharset(string name)
        {
            throw new NotImplementedException();
        }

        public override void SetComment(string comment)
        {
            throw new NotImplementedException();
        }
    }
}
