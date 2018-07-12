using System;
using System.Configuration;
using System.Diagnostics;

namespace CPREnvoy
{
    static class Program
    {
        public static string serviceName = ConfigurationManager.AppSettings.Get("ecpr_service_name");

        public static string getVersion()
        {
            System.Reflection.Assembly assembly = System.Reflection.Assembly.GetExecutingAssembly();
            return FileVersionInfo.GetVersionInfo(assembly.Location).FileVersion;
        }

        static void Main(string[] args)
        {
            Config eConfig = new Config();
            Logger logger = new Logger(eConfig);

            Console.WriteLine("Press any key to close console ...");
            Console.ReadKey();
        }
    }
}
