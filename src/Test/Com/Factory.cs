using Dapper.Sharding;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Test.Com;

namespace Test
{
    public class Factory
    {
        //Must singleton mode(必须是单例模式)
        public static IClient Client = ClientFactory.CreateClient(DataBaseType.MySql, "server=127.0.0.1;user=root");

        public static IDatabase Db
        {
            get
            {
                return Client.GetDatabase("test");
            }
        }

        public static ITableManager TableManager
        {
            get
            {
                return Db.GetTableManager("People");
            }
        }

        public static ITable<People> peopleTable
        {
            get 
            {
                return Db.GetTable<People>("People"); //Multi threading is not safe, you must new it
            }
        }

        public static ITable<Student> studentTable
        {
            get
            {
                return Db.GetTable<Student>("Student");
            }
        }

        public static ITable<Teacher> teacherTable
        {
            get
            {
                return Db.GetTable<Teacher>("Teacher");
            }
        }


    }
}
