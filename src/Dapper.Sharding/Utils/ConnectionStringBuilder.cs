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
            sb.Append($"server={config.Server}");
            sb.Append($";uid={config.UserId}");
            if (!string.IsNullOrEmpty(config.Password))
            {
                sb.Append($";pwd={config.Password}");
            }
            if (!string.IsNullOrEmpty(databaseName))
            {
                sb.Append($";database={databaseName}");
            }
            if (config.Port != 0)
            {
                sb.Append($";port={config.Port}");
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
            sb.Append($"server={config.Server}");
            if (config.Port != 0)
            {
                sb.Append($",{config.Port}");
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
            sb.Append($"server={config.Server}");
            sb.Append($";uid={config.UserId}");
            if (!string.IsNullOrEmpty(config.Password))
            {
                sb.Append($";pwd={config.Password}");
            }
            if (!string.IsNullOrEmpty(databaseName))
            {
                sb.Append($";database={databaseName}");
            }
            if (config.Port != 0)
            {
                sb.Append($";port={config.Port}");
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
            sb.Append($"data source={config.Server}");
            if (config.Port != 0)
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
            sb.Append($"data source={config.Server}");
            if (config.Port != 0)
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

    }
}
