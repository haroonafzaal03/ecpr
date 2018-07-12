using System;
using System.Collections.Generic;
using System.Runtime.Caching;
using System.Collections;

namespace CPREnvoy
{
    public static class DataChangesCache
    {
        private static MemoryCache memoryCache = new MemoryCache("DataChangesCache");
        private static List<string> arrPOSTing = new List<string>();

        public static void AddItem(string pKey, object pVal)
        {
            memoryCache.Add(pKey, pVal, DateTimeOffset.MaxValue);
        }

        public static object GetItem(string pKey)
        {
            var pVal = memoryCache[pKey];

            if (pVal != null)
                return pVal;

            return null;
        }

        public static void RemoveItem(string pKey) {
            memoryCache.Remove(pKey);
        }

        public static List<string> Keys()
        {
            List<string> pKeys = new List<string>();
            if (memoryCache.GetCount() != 0)
            {
                IDictionaryEnumerator dictionaryEnumerator = (IDictionaryEnumerator)((IEnumerable)memoryCache).GetEnumerator();
                while (dictionaryEnumerator.MoveNext())
                {
                    pKeys.Add((string)dictionaryEnumerator.Key);
                }
            }

            pKeys.Sort();

            return pKeys;
        }

        public static bool isPOSTing(string pKey)
        {
            return arrPOSTing.Contains(pKey);
        }

        public static void setPOSTing(string pKey)
        {
            arrPOSTing.Add(pKey);
        }

        public static void removePOSTing(string pKey)
        {
            arrPOSTing.Remove(pKey);
        }

        private static void cleanup()
        {
            arrPOSTing.Clear();
            arrPOSTing = null;
            memoryCache = null;
            GC.Collect();
        }
    }
}
