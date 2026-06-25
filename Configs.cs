using System;
using System.Reflection.Metadata;

namespace MyProgram
{
    internal static class Config
    {
        private static readonly string BaseWay = AppContext.BaseDirectory;

        public static readonly string csvConfigPath = Path.Combine(BaseWay, "source.csv");
        public static readonly string addressesDataBasePath = Path.Combine(BaseWay, "addressDB.bin");
        public static readonly string todayStatisticDataBasePath = Path.Combine(BaseWay, "todaystatDB.bin");
        public static readonly string globalStatisticDataBasePath = Path.Combine(BaseWay, "globalstatDB.bin");
        public static readonly string appLogPath = Path.Combine(BaseWay, "app.log");
        //                                      часы    +   минуты
        public static readonly int timeToSave = 23 * 60 + 59;
    }
}