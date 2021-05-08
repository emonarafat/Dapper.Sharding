using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dapper.Sharding
{
    public class ConnectionStringBuilder
    {
        public static string BuilderMySql(DataBaseConfig config, string databaseName = null)
        {
            var sb = new StringBuilder();
            if (string.IsNullOrEmpty(config.Server))
            {
                config.Server = "127.0.0.1";
            }
            sb.Append($"server={config.Server}");
            if (config.Port != 0) //3306
            {
                sb.Append($";port={config.Port}");
            }
            if (string.IsNullOrEmpty(config.UserId))
            {
                config.UserId = "root";
            }
            sb.Append($";uid={config.UserId}");
            if (!string.IsNullOrEmpty(config.Password))
            {
                sb.Append($";pwd={config.Password}");
            }
            if (!string.IsNullOrEmpty(databaseName))
            {
                sb.Append($";database={databaseName}");
            }
            if (config.MinPoolSize != 0)
            {
                sb.Append($";min pool size={config.MinPoolSize}");
            }
            if (config.MaxPoolSize != 0)
            {
                sb.Append($";max pool size={config.MaxPoolSize}");
            }
            if (config.TimeOut != 0)
            {
                sb.Append($";connect timeout={config.TimeOut}");
            }
            if (!string.IsNullOrEmpty(config.CharSet))
            {
                sb.Append($";charset={config.CharSet}");
            }
            return sb.ToString();
        }

        public static string BuilderSqlServer(DataBaseConfig config, string databaseName = null)
        {
            var sb = new StringBuilder();
            if (string.IsNullOrEmpty(config.Server))
            {
                config.Server = "127.0.0.1";
            }
            sb.Append($"server={config.Server}");
            if (config.Port != 0) //1433
            {
                sb.Append($",{config.Port}");
            }
            if (string.IsNullOrEmpty(config.UserId))
            {
                config.UserId = "sa";
            }
            sb.Append($";uid={config.UserId}");

            if (!string.IsNullOrEmpty(config.Password))
            {
                sb.Append($";pwd={config.Password}");
            }
            if (!string.IsNullOrEmpty(databaseName))
            {
                sb.Append($";database={databaseName}");
            }
            if (config.MinPoolSize != 0)
            {
                sb.Append($";min pool size={config.MinPoolSize}");
            }
            if (config.MaxPoolSize != 0)
            {
                sb.Append($";max pool size={config.MaxPoolSize}");
            }
            if (config.TimeOut != 0)
            {
                sb.Append($";timeout={config.TimeOut}");
            }
            return sb.ToString();
        }

        public static string BuilderPostgresql(DataBaseConfig config, string databaseName = null)
        {
            var sb = new StringBuilder();
            if (string.IsNullOrEmpty(config.Server))
            {
                config.Server = "127.0.0.1";
            }
            sb.Append($"server={config.Server}");
            if (config.Port != 0) //5432
            {
                sb.Append($";port={config.Port}");
            }
            if (string.IsNullOrEmpty(config.UserId))
            {
                config.UserId = "postgres";
            }
            sb.Append($";uid={config.UserId}");
            if (!string.IsNullOrEmpty(config.Password))
            {
                sb.Append($";pwd={config.Password}");
            }
            if (!string.IsNullOrEmpty(databaseName))
            {
                sb.Append($";database={databaseName}");
            }
            if (config.MinPoolSize != 0)
            {
                sb.Append($";minpoolsize={config.MinPoolSize}");
            }
            if (config.MaxPoolSize != 0)
            {
                sb.Append($";maxpoolsize={config.MaxPoolSize}");
            }
            if (config.TimeOut != 0)
            {
                sb.Append($";commandtimeout={config.TimeOut}");
            }
            if (!string.IsNullOrEmpty(config.CharSet))
            {
                sb.Append($";encoding={config.CharSet}");
            }
            return sb.ToString();
        }

        public static string BuilderOracleSysdba(DataBaseConfig config)
        {
            var sb = new StringBuilder();
            if (string.IsNullOrEmpty(config.Server))
            {
                config.Server = "127.0.0.1";
            }
            sb.Append($"data source={config.Server}");
            if (config.Port != 0) //1521
            {
                sb.Append($":{config.Port}");
            }
            if (!string.IsNullOrEmpty(config.Oracle_ServiceName))
            {
                sb.Append($"/{config.Oracle_ServiceName}");
            }
            sb.Append($";user id={config.Oracle_SysUserId}");
            if (!string.IsNullOrEmpty(config.Oracle_SysPassword))
            {
                sb.Append($";password={config.Oracle_SysPassword}");
            }
            if (config.MinPoolSize != 0)
            {
                sb.Append($";min pool size={config.MinPoolSize}");
            }
            if (config.MaxPoolSize != 0)
            {
                sb.Append($";max pool size={config.MaxPoolSize}");
            }
            if (config.TimeOut != 0)
            {
                sb.Append($";connect timeout={config.TimeOut}");
            }
            sb.Append(";dba privilege=sysdba");
            return sb.ToString();
        }

        public static string BuilderOracle(DataBaseConfig config, string userId = null)
        {
            var sb = new StringBuilder();
            if (string.IsNullOrEmpty(config.Server))
            {
                config.Server = "127.0.0.1";
            }
            sb.Append($"data source={config.Server}");
            if (config.Port != 0) //1521
            {
                sb.Append($":{config.Port}");
            }
            if (!string.IsNullOrEmpty(config.Oracle_ServiceName))
            {
                sb.Append($"/{config.Oracle_ServiceName}");
            }
            if (userId != null && userId.ToLower() != config.UserId)
            {
                config.UserId = userId;
            }
            sb.Append($";user id={config.UserId}");

            if (!string.IsNullOrEmpty(config.Password))
            {
                sb.Append($";password={config.Password}");
            }
            if (config.MinPoolSize != 0)
            {
                sb.Append($";min pool size={config.MinPoolSize}");
            }
            if (config.MaxPoolSize != 0)
            {
                sb.Append($";max pool size={config.MaxPoolSize}");
            }
            if (config.TimeOut != 0)
            {
                sb.Append($";connect timeout={config.TimeOut}");
            }
            return sb.ToString();
        }

        public static string BuilderClickHouse(DataBaseConfig config, string databaseName = null)
        {
            var sb = new StringBuilder();
            if (string.IsNullOrEmpty(config.Server))
            {
                config.Server = "127.0.0.1";
            }
            sb.Append($"Host={config.Server}");
            if (config.Port == 0) //9000
            {
                config.Port = 9000;
            }
            sb.Append($";Port={config.Port}");
            if (string.IsNullOrEmpty(config.UserId))
            {
                config.UserId = "default";
            }
            sb.Append($";User={config.UserId}");
            if (!string.IsNullOrEmpty(config.Password))
            {
                sb.Append($";Passsword={config.Password}");
            }
            if (string.IsNullOrEmpty(databaseName))
            {
                databaseName = "default";
            }
            sb.Append($"Database={databaseName}");

            sb.Append(";Compress=True");
            return sb.ToString();
        }
    }
}
