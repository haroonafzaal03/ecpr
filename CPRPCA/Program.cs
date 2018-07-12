using System;
using System.Threading;
using System.Reflection;
using System.ServiceProcess;
using System.Configuration.Install;
using System.Collections;
using System.Diagnostics;
using System.Configuration;

namespace CPREnvoy
{
    [System.ComponentModel.DesignerCategory("Code")]

    [System.ComponentModel.RunInstaller(true)]
    public sealed class ECPRInstallerProcess : ServiceProcessInstaller
    {
        public ECPRInstallerProcess()
        {
            this.Account = ServiceAccount.NetworkService;
        }
    }

    [System.ComponentModel.RunInstaller(true)]
    public sealed class ECPRServiceInstaller : ServiceInstaller
    {
        public ECPRServiceInstaller()
        {
            this.Description = String.Format("{0} (Version: {1})", ConfigurationManager.AppSettings.Get("ecpr_service_description"), Program.getVersion()); //"Envoy ECPR Patient sync"
            this.DisplayName = ConfigurationManager.AppSettings.Get("ecpr_service_displayname"); //"Envoy ECPR Patient sync"
            this.ServiceName = Program.serviceName;
            this.StartType = ServiceStartMode.Automatic;
        }

        protected override void OnAfterInstall(System.Collections.IDictionary savedState)
        {
            base.OnAfterInstall(savedState);
            int exitCode = Program.executeCommand("net ", String.Format("start \"{0}\"", this.ServiceName));

            Console.WriteLine("Trying to START service ...");
            if (exitCode != 0)
            {
                Console.WriteLine("== FAILED to START service!");
                Console.WriteLine("== Try to start service manually by use this command to the see details of error: net start \"{0}\"", this.ServiceName);
            } else
            {
                Console.WriteLine("Starting up service successfully!");
            }
        }

        protected override void OnCommitted(System.Collections.IDictionary savedState)
        {
            base.OnCommitted(savedState);
            int exitCode = Program.executeCommand("sc ", String.Format("failure \"{0}\" reset= 0 actions= restart/60000", this.ServiceName));
        }
    }

    static class Program
    {
        public static string serviceName = ConfigurationManager.AppSettings.Get("ecpr_service_name");
        static void RunInteractive(ServiceBase[] servicesToRun)
        {
            var isStarted = false;

            Console.WriteLine();
            Console.WriteLine("Running in interactive mode.");
            Console.WriteLine();

            MethodInfo onStartMethod = typeof(ServiceBase).GetMethod("OnStart",
                BindingFlags.Instance | BindingFlags.NonPublic);

            foreach (ServiceBase service in servicesToRun)
            {
                Console.Write("Starting {0}...", service.ServiceName);
                onStartMethod.Invoke(service, new object[] { new string[] { } });
                Console.Write("Started");
                isStarted = true;
            }

            if (isStarted)
            {
                Console.WriteLine();
                Console.WriteLine("Press any key to stop the services and end the process...");
                Console.ReadKey();
                Console.WriteLine();

                MethodInfo onStopMethod = typeof(ServiceBase).GetMethod("OnStop",
                    BindingFlags.Instance | BindingFlags.NonPublic);

                foreach (ServiceBase service in servicesToRun)
                {
                    Console.Write("Stopping {0}...", service.ServiceName);
                    onStopMethod.Invoke(service, null);
                    Console.WriteLine("Stopped");
                    Environment.Exit(0);
                }
            }
            else
            {
                Console.WriteLine();
                Console.WriteLine("Cannot start service because error(s) has occurred");
                Console.WriteLine();
                Console.WriteLine("Press any key to close console ...");
                Console.ReadKey();
            }
            Console.WriteLine("All services stopped.");
        }

        static int Main(string[] args)
        {
            string helpers = @"
    * Usage: ECPR.exe <option>
    * Options:
            -i | --install      Install ECPR service.
            -u | --uninstall    Uninstall ECPR service.
            -c | --cleanup      Delete everything relate to ECPR service (triggers, log tables, ...) in cpr database.
            -v | --version      Show current version.
            -V | --verify       Verify all things are passed (connect to sql service database, ...).
            -h | --help

";

            if (Environment.UserInteractive)
            {
                if (args.Length != 0)
                {
                    string arg = args[0].ToLowerInvariant().Substring(0, 2);

                    switch (arg)
                    {
                        case "-i":
                        case "--install":
                            if (serviceIsInstalled())
                            {
                                Console.WriteLine("The service is already installed. Please uninstall it first.");
                                return 0;
                            }
                            ensureMMCClosed();
                            Console.WriteLine("Installing ECPR service ...");
                            selfInstaller(false, args);
                            return 0;
                        case "-u":
                        case "--uninstall":
                            if (!serviceIsInstalled())
                            {
                                Console.WriteLine("The service is not installed. Please install it first.");
                                return 0;
                            }
                            ensureMMCClosed();
                            Console.WriteLine("Uninstalling ECPR service ...");
                            selfInstaller(true, args);
                            return 0;
                        case "-c":
                        case "--cleanup":
                            if (serviceIsInstalled())
                            {
                                Console.WriteLine("The service is installed. Please uninstall it first.");
                                return 0;
                            }

                            var uninstall = new Uninstall(new Config());
                            uninstall.runTasks();
                            uninstall = null;
                            return 0;
                        case "-v":
                        case "--version":
                            Console.WriteLine(getVersion());
                            return 0;
                        case "-V":
                        case "--verify":
                            return 0;
                        case "-h":
                        case "--help":
                            Console.WriteLine(helpers);
                            return 0;
                        default:
                            return 1;
                    }
                }
                else
                {
                    RunInteractive(initializeService());
                }
            }
            else
            {
                ServiceBase.Run(initializeService());
            }

            return 0;
        }

        private static void ensureMMCClosed()
        {
            Console.WriteLine("Ensure all Services windows are closed before do install or uninstall the service.");
            executeCommand("taskkill ", "/F /IM mmc.exe");
        }

        public static int executeCommand(string cmd, string arg)
        {
            ProcessStartInfo processStartInfo = new ProcessStartInfo(cmd, arg);
            processStartInfo.CreateNoWindow = true;
            processStartInfo.UseShellExecute = false;

            processStartInfo.RedirectStandardOutput = true;
            processStartInfo.RedirectStandardError = true;

            Process process = Process.Start(processStartInfo);
            process.WaitForExit();

            int exitCode = process.ExitCode;

            process.Close();

            return exitCode;
        }

        public static string getVersion()
        {
            System.Reflection.Assembly assembly = System.Reflection.Assembly.GetExecutingAssembly();
            return FileVersionInfo.GetVersionInfo(assembly.Location).FileVersion;
        }

        private static ServiceBase[] initializeService()
        {
            Config eConfig = new Config();
            Logger logger = new Logger(eConfig);

            Envoy eService = new Envoy(eConfig, logger);

            if (eService.isReadyToBeStarted)
            {
                return new ServiceBase[] { eService };
            }

            return new ServiceBase[] {};
        }

        private static bool serviceIsInstalled()
        {
            foreach (ServiceController srv in ServiceController.GetServices())
            {
                if (srv.ServiceName == serviceName)
                {
                    return true;
                }
            }

            return false;
        }

        static void selfInstaller(bool undo, string[] args)
        {
            try
            {
                using (AssemblyInstaller inst = new AssemblyInstaller(typeof(Program).Assembly, args))
                {
                    IDictionary state = new Hashtable();
                    inst.UseNewContext = true;
                    try
                    {
                        if (undo)
                        {
                            inst.Uninstall(state);
                        }
                        else
                        {
                            inst.Install(state);
                            inst.Commit(state);
                        }
                    }
                    catch
                    {
                        try
                        {
                            inst.Rollback(state);
                        }
                        catch { }
                        throw;
                    }
                }
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine(ex.Message);
            }
        }

    }
}
