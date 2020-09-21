//using System;
//using System.Security.Cryptography;
//using System.Text;

//namespace Dapper.Sharding
//{
//    internal class HashAlgorithm
//    {
//        private static byte[] GetMd5(string k)
//        {
//            byte[] keyBytes;
//            using (var md5 = new MD5CryptoServiceProvider())
//            {
//                keyBytes = md5.ComputeHash(Encoding.UTF8.GetBytes(k));
//                md5.Clear();
//            }
//            return keyBytes;
//        }

//        private static long GetHash(byte[] digest)
//        {
//            long rv = ((long)(digest[3] & 0xFF) << 24)
//                    | ((long)(digest[2] & 0xFF) << 16)
//                    | ((long)(digest[1] & 0xFF) << 8)
//                    | ((long)digest[0] & 0xFF);

//            return rv & 0xffffffffL;
//        }

//        public static long GetHash(string key)
//        {
//            return GetHash(GetMd5(key));
//        }

//        public static int Mod(string key, int count)
//        {
//            return GetHash(key) % count;
//        }

//        public static int HashCode(string key)
//        {
//            int HashCode;
//            byte[] Md5Bype;
//            using (var md5 = new MD5CryptoServiceProvider())
//            {
//                Md5Bype = md5.ComputeHash(Encoding.UTF8.GetBytes(key));
//            }
//            HashCode = BitConverter.ToInt32(Md5Bype, 0);
//            if (HashCode < 0)
//            {
//                HashCode = int.MaxValue + HashCode;
//            }
//            return Math.Abs(HashCode);
//        }

//        public static int Mod(string key, int count)
//        {
//            return HashCode(key) % count;
//        }


//    }
//}
