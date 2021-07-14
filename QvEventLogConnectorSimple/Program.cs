using System;
using QvEventLogConnectorSimple;

namespace QvEventLogConnectorElaborate
{
    static class Program
    {
        [STAThread]
        static void Main(string[] args)
        {
            QvAditiConectorServer teste = new QvAditiConectorServer();
            teste.CreateConnection();
            if (args != null && args.Length >= 2)
            {
                new QvAditiConectorServer().Run(args[0], args[1]);
            }       
        }
    }
}
