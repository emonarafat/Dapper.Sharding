using Npgsql;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dapper.Sharding
{
    internal class PostgreClient : IClient
    {

        public PostgreClient(DataBaseConfig config) : base(DataBaseType.Postgresql, config)
        {
            ConnectionString = ConnectionStringBuilder.BuilderPostgresql(config);
        }

        public override string ConnectionString { get; }

        #region protected method

        protected override IDatabase CreateIDatabase(string name)
        {
            return new PostgreDatabase(name, this);
        }

        #endregion

        public override void CreateDatabase(string name, bool useGis = false, string ext = null)
        {
            Execute($"CREATE DATABASE {name}");

            if (useGis)
            {
                if (string.IsNullOrEmpty(ext))
                {
                    ext = "CREATE EXTENSION IF NOT EXISTS postgis";
                }
                else if (ext == "1")
                {
                    var sb = new StringBuilder();
                    sb.Append("CREATE EXTENSION IF NOT EXISTS postgis;");
                    sb.Append("CREATE EXTENSION IF NOT EXISTS postgis_raster;");
                    sb.Append("CREATE EXTENSION IF NOT EXISTS postgis_sfcgal;");
                    sb.Append("CREATE EXTENSION IF NOT EXISTS postgis_topology;");
                    ext = sb.ToString();
                }
                else if (ext == "2")
                {
                    var sb = new StringBuilder();
                    sb.Append("CREATE EXTENSION IF NOT EXISTS postgis;");
                    sb.Append("CREATE EXTENSION IF NOT EXISTS postgis_raster;");
                    sb.Append("CREATE EXTENSION IF NOT EXISTS postgis_sfcgal;");
                    sb.Append("CREATE EXTENSION IF NOT EXISTS postgis_topology;");

                    sb.Append("CREATE EXTENSION IF NOT EXISTS pgrouting;");
                    sb.Append("CREATE EXTENSION IF NOT EXISTS ogr_fdw;");

                    ext = sb.ToString();
                }
                else if (ext == "3")
                {
                    var sb = new StringBuilder();
                    sb.Append("CREATE EXTENSION IF NOT EXISTS postgis;");
                    sb.Append("CREATE EXTENSION IF NOT EXISTS postgis_raster;");
                    sb.Append("CREATE EXTENSION IF NOT EXISTS postgis_sfcgal;");
                    sb.Append("CREATE EXTENSION IF NOT EXISTS postgis_topology;");

                    sb.Append("CREATE EXTENSION IF NOT EXISTS ogr_fdw;");
                    sb.Append("CREATE EXTENSION IF NOT EXISTS pgrouting;");

                    sb.Append("CREATE EXTENSION IF NOT EXISTS address_standardizer;");
                    sb.Append("CREATE EXTENSION IF NOT EXISTS address_standardizer_data_us;");
                    sb.Append("CREATE EXTENSION IF NOT EXISTS fuzzystrmatch;");
                    sb.Append("CREATE EXTENSION IF NOT EXISTS postgis_tiger_geocoder;");
                    ext = sb.ToString();
                }
                var db = CreateIDatabase(name);
                db.Execute(ext);
            }
        }

        public override void DropDatabase(string name)
        {
            Execute($"DROP DATABASE IF EXISTS {name}");
            DataBaseCache.TryRemove(name, out _);
        }

        public override bool ExistsDatabase(string name)
        {
           return ExecuteScalar<int>($"SELECT COUNT(1) FROM pg_database WHERE datname = '{name}'") > 0;
        }

        public override IDbConnection GetConn()
        {
            var conn = new NpgsqlConnection(ConnectionString);
            if (conn.State == ConnectionState.Closed)
            {
                try
                {
                    conn.Open();
                }
                catch (Exception ex)
                {
                    conn.Dispose();
                    throw ex;
                }
            }
            return conn;
        }

        public override async Task<IDbConnection> GetConnAsync()
        {
            var conn = new NpgsqlConnection(ConnectionString);
            if (conn.State == ConnectionState.Closed)
            {
                try
                {
                    await conn.OpenAsync();
                }
                catch (Exception ex)
                {
                    await conn.DisposeAsync();
                    throw ex;
                }
            }
            return conn;
        }

        public override IEnumerable<string> ShowDatabases()
        {
            return Query<string>("select pg_database.datname from pg_database");
        }

        public override IEnumerable<string> ShowDatabasesExcludeSystem()
        {
            return ShowDatabases().Where(w => w != "template1" && w != "template0");
        }
    }
}
