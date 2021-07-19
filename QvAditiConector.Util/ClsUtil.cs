using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QvEventLogConnectorSimple.Util
{
    public class ClsUtil
    {
        public string[] RecuperaParametrosConnectionString(string parametros)
        {
            string[] separadores = new string[] { "-*" };
            return parametros.Split(separadores, 0);
        }
    }
}