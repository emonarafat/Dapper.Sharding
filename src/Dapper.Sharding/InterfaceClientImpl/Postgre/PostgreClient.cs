using Npgsql;
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

        public override void CreateDatabase(string name, bool useGis = false, string gisExt = null)
        {
            using (var conn = GetConn())
            {
                conn.Execute($"CREATE DATABASE {name}");
            }

            if (useGis)
            {
                if (string.IsNullOrEmpty(gisExt))
                {
                    gisExt = "CREATE EXTENSION IF NOT EXISTS postgis";
                }
                else if (gisExt == "1")
                {
                    var sb = new StringBuilder();
                    sb.Append("CREATE EXTENSION IF NOT EXISTS postgis;");
                    sb.Append("CREATE EXTENSION IF NOT EXISTS postgis_raster;");
                    sb.Append("CREATE EXTENSION IF NOT EXISTS postgis_sfcgal;");
                    sb.Append("CREATE EXTENSION IF NOT EXISTS postgis_topology;");
                    gisExt = sb.ToString();
                }
                else if (gisExt == "2")
                {
                    var sb = new StringBuilder();
                    sb.Append("CREATE EXTENSION IF NOT EXISTS postgis;");
                    sb.Append("CREATE EXTENSION IF NOT EXISTS postgis_raster;");
                    sb.Append("CREATE EXTENSION IF NOT EXISTS postgis_sfcgal;");
                    sb.Append("CREATE EXTENSION IF NOT EXISTS postgis_topology;");

                    sb.Append("CREATE EXTENSION IF NOT EXISTS pgrouting;");
                    sb.Append("CREATE EXTENSION IF NOT EXISTS ogr_fdw;");

                    gisExt = sb.ToString();
                }
                else if (gisExt == "3")
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
                    gisExt = sb.ToString();
                }
                var db = CreateIDatabase(name);
                db.Using(conn =>
                {
                    conn.Execute(gisExt);
                });
            }
        }

        public override void DropDatabase(string name)
        {
            using (var conn = GetConn())
            {
                conn.Execute($"DROP DATABASE IF EXISTS {name}");
            }
            DataBaseCache.TryRemove(name, out _);
        }

        public override bool ExistsDatabase(string name)
        {
            using (var conn = GetConn())
            {
                return conn.ExecuteScalar<int>($"SELECT COUNT(1) FROM pg_database WHERE datname = '{name}'") > 0;

            }
        }

        public override IDbConnection GetConn()
        {
            var conn = new NpgsqlConnection(ConnectionString);
            if (conn.State != ConnectionState.Open)
                conn.Open();
            return conn;
        }

        public override async Task<IDbConnection> GetConnAsync()
        {
            var conn = new NpgsqlConnection(ConnectionString);
            if (conn.State != ConnectionState.Open)
                await conn.OpenAsync();
            return conn;
        }

        public override IEnumerable<string> ShowDatabases()
        {
            using (var conn = GetConn())
            {
                return conn.Query<string>("select pg_database.datname from pg_database");
            }
        }

        public override IEnumerable<string> ShowDatabasesExcludeSystem()
        {
            return ShowDatabases().Where(w => w != "template1" && w != "template0");
        }
    }
}
