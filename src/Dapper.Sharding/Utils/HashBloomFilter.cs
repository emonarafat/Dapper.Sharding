namespace Dapper.Sharding
{
    public class HashBloomFilter
    {
        // BKDR Hash Function  92.64
        public static int BKDRHash(string str)
        {
            int seed = 131; // 31 131 1313 13131 131313 etc..
            int hash = 0;
            int count;
            char[] bitarray = str.ToCharArray();
            count = bitarray.Length;
            while (count > 0)
            {
                hash = hash * seed + (bitarray[bitarray.Length - count]);
                count--;
            }
            return (hash & 0x7FFFFFFF);
        }

        public static int Mod(string str, int count)
        {
            return BKDRHash(str) % count;
        }

        ////AP hash function 86.28
        //public static int APHash(string str)
        //{
        //    int hash = 0;
        //    int i;
        //    int count;
        //    char[] bitarray = str.ToCharArray();
        //    count = bitarray.Length;
        //    for (i = 0; i < count; i++)
        //    {
        //        if ((i & 1) == 0)
        //        {
        //            hash ^= ((hash << 7) ^ (bitarray[i]) ^ (hash >> 3));
        //        }
        //        else
        //        {
        //            hash ^= (~((hash << 11) ^ (bitarray[i]) ^ (hash >> 5)));
        //        }
        //        count--;
        //    }

        //    return (hash & 0x7FFFFFFF);
        //}

        //// DJB Hash Function 83.43
        //public static int DJBHash(string str)
        //{
        //    int hash = 5381;
        //    int i;
        //    int count;
        //    char[] bitarray = str.ToCharArray();
        //    count = bitarray.Length;
        //    while (count > 0)
        //    {
        //        hash += (hash << 5) + (bitarray[bitarray.Length - count]);
        //        count--;
        //    }

        //    return (hash & 0x7FFFFFFF);
        //}

        //// JS Hash Function 81.94
        //public static int JSHash(string str)
        //{
        //    int hash = 1315423911;
        //    int count;
        //    char[] bitarray = str.ToCharArray();
        //    count = bitarray.Length;
        //    while (count > 0)
        //    {
        //        hash ^= ((hash << 5) + (bitarray[bitarray.Length - count]) + (hash >> 2));
        //        count--;
        //    }

        //    return (hash & 0x7FFFFFFF);
        //}

        //// RS Hash Function  75.96
        //public static int RSHash(string str)
        //{
        //    int b = 378551;
        //    int a = 63689;
        //    int hash = 0;

        //    int i;
        //    int count;
        //    char[] bitarray = str.ToCharArray();
        //    count = bitarray.Length;
        //    while (count > 0)
        //    {
        //        hash = hash * a + (bitarray[bitarray.Length - count]);
        //        a *= b;
        //        count--;
        //    }

        //    return (hash & 0x7FFFFFFF);
        //}

        ////SDBM Hash function  72.41
        //public static int SDBMHash(string str)
        //{
        //    int hash = 0;
        //    int i;
        //    int count;
        //    char[] bitarray = str.ToCharArray();
        //    count = bitarray.Length;

        //    while (count > 0)
        //    {
        //        // equivalent to: hash = 65599*hash + (*str++);
        //        hash = (bitarray[bitarray.Length - count]) + (hash << 6) + (hash << 16) - hash;
        //        count--;
        //    }

        //    return (hash & 0x7FFFFFFF);
        //}

        //// P. J. Weinberger Hash Function 21.95
        //public static int PJWHash(string str)
        //{
        //    int BitsInUnignedInt = (int)(sizeof(int) * 8);
        //    int ThreeQuarters = (int)((BitsInUnignedInt * 3) / 4);
        //    int OneEighth = (int)(BitsInUnignedInt / 8);
        //    int hash = 0;
        //    unchecked
        //    {
        //        int HighBits = (int)(0xFFFFFFFF) << (BitsInUnignedInt - OneEighth);
        //        int test = 0;
        //        int count;
        //        char[] bitarray = str.ToCharArray();
        //        count = bitarray.Length;
        //        while (count > 0)
        //        {
        //            hash = (hash << OneEighth) + (bitarray[bitarray.Length - count]);
        //            if ((test = hash & HighBits) != 0)
        //            {
        //                hash = ((hash ^ (test >> ThreeQuarters)) & (~HighBits));
        //            }
        //            count--;
        //        }
        //    }
        //    return (hash & 0x7FFFFFFF);
        //}

        //// ELF Hash Function  21.95
        //public static int ELFHash(string str)
        //{
        //    int hash = 0;
        //    int x = 0;
        //    int i;
        //    int count;
        //    char[] bitarray = str.ToCharArray();
        //    count = bitarray.Length;
        //    unchecked
        //    {
        //        while (count > 0)
        //        {
        //            hash = (hash << 4) + (bitarray[bitarray.Length - count]);
        //            if ((x = hash & (int)0xF0000000) != 0)
        //            {
        //                hash ^= (x >> 24);
        //                hash &= ~x;
        //            }
        //            count--;
        //        }
        //    }
        //    return (hash & 0x7FFFFFFF);
        //}




    }
}
