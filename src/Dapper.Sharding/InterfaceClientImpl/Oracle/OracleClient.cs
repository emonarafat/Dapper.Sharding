using Oracle.ManagedDataAccess.Client;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Threading.Tasks;

namespace Dapper.Sharding
{
    internal class OracleClient : IClient
    {

        public OracleClient(DataBaseConfig config) : base(DataBaseType.Oracle, config)
        {
            if (config.Oracle_TableSpace_Mb == 0)
                config.Oracle_TableSpace_Mb = 1;
            if (config.Oracle_TableSpace_NextMb == 0)
                config.Oracle_TableSpace_NextMb = 1;
            if (!string.IsNullOrEmpty(config.Oracle_DatabaseDirectory))
            {
                if (!Directory.Exists(config.Oracle_DatabaseDirectory))
                {
                    Directory.CreateDirectory(config.Oracle_DatabaseDirectory);
                }
            }
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
            var upName = name.ToUpper();
            string dbpath;
            if (!string.IsNullOrEmpty(Config.Oracle_DatabaseDirectory))
            {
                dbpath = Path.Combine(Config.Oracle_DatabaseDirectory, upName + ".DBF");
            }
            else
            {
                dbpath = upName + ".DBF";
            } 

            var sql1 = $"create user {upName} identified by {Config.Password}";
            var sql2 = $"create tablespace {upName} datafile '{dbpath}' size {Config.Oracle_TableSpace_Mb}m autoextend on next {Config.Oracle_TableSpace_NextMb}m";
            var sql3 = $"alter user {upName} default tablespace {upName}";
            var sql4 = $"grant create session,create table,unlimited tablespace to {upName}";
            var sql5 = $"alter user {upName} account unlock";
            var sql6 = $"grant connect,resource,dba to {upName}";
            using (var conn = GetConn())
            {
                conn.Execute(sql1);
                conn.Execute(sql2);
                conn.Execute(sql3);
                conn.Execute(sql4);
                conn.Execute(sql5);
                conn.Execute(sql6);
            }
        }

        public override void DropDatabase(string name)
        {
            var sql = $@"DROP USER {name.ToUpper()} CASCADE";
            var sql2 = $"DROP TABLESPACE {name.ToUpper()} INCLUDING CONTENTS AND DATAFILES";
            using (var conn = GetConn())
            {   
                conn.Execute(sql);   
                conn.Execute(sql2);
            }
            DataBaseCache.TryRemove(name.ToLower(), out _);
        }

        public override bool ExistsDatabase(string name)
        {
            using (var conn = GetConn())
            {
                return conn.ExecuteScalar<long>($"SELECT COUNT(1) FROM dba_users WHERE USERNAME='{name.ToUpper()}'") > 0;
            }
        }

        public override IDbConnection GetConn()
        {
            var conn = new OracleConnection(ConnectionString);
            if (conn.State != ConnectionState.Open)
                conn.Open();
            return conn;
        }

        public override async Task<IDbConnection> GetConnAsync()
        {
            var conn = new OracleConnection(ConnectionString);
            if (conn.State != ConnectionState.Open)
                await conn.OpenAsync();
            return conn;
        }

        public override IEnumerable<string> ShowDatabases()
        {
            using (var conn = GetConn())
            {
                return conn.Query<string>("SELECT USERNAME FROM dba_users");
            }
        }

        public override IEnumerable<string> ShowDatabasesExcludeSystem()
        {
            using (var conn = GetConn())
            {
                return conn.Query<string>("SELECT USERNAME FROM dba_users WHERE DEFAULT_TABLESPACE NOT IN('SYSTEM','SYSAUX','USERS')");
            }
        }

    }
}
