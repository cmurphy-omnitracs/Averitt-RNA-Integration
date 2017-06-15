namespace Averitt_RNA
{
    static class Program
    {
        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        static void Main()
        {
#if DEBUG
            MainService debugService = new MainService();
            debugService.OnDebug();
            System.Threading.Thread.Sleep(System.Threading.Timeout.Infinite);
#else
            System.ServiceProcess.ServiceBase[] ServicesToRun;
            ServicesToRun = new System.ServiceProcess.ServiceBase[]
            {
                new MainService()
            };
            System.ServiceProcess.ServiceBase.Run(ServicesToRun);
#endif
        }
    }
}
