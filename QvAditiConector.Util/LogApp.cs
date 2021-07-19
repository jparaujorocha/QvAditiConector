using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace QvEventLogConnectorSimple.Util
{
    public class LogApp
    {
        private string _caminhoAplicacao;
        private string _nomeArquivo;
        private string _caminhoArquivo;

        public LogApp()
        {
            _caminhoAplicacao = AppDomain.CurrentDomain.BaseDirectory + "Log" + @"\";
            _nomeArquivo = "Log - " + System.DateTime.Now.Year.ToString() + "-" + System.DateTime.Now.Month.ToString() + "-" + System.DateTime.Now.Day.ToString() + ".txt";

            _caminhoArquivo = Path.Combine(_caminhoAplicacao, _nomeArquivo);

            if (Directory.Exists(_caminhoAplicacao) == false)
            {
                Directory.CreateDirectory(_caminhoAplicacao);
                FileStream arquivo = File.Create(_caminhoArquivo);
                arquivo.Close();
            }

            if (File.Exists(_caminhoArquivo) == false)
            {
                FileStream arquivo = File.Create(_caminhoArquivo);
                arquivo.Close();
            }
        }

        public void CriarLog(string strMensagem)
        {
            try
            {
                using (StreamWriter w = File.AppendText(_caminhoArquivo))
                {
                    AppendLog(strMensagem, w);
                }

            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        private static void AppendLog(string logMensagem, TextWriter txtWriter)
        {
            try
            {
                txtWriter.WriteLine($"{DateTime.Now.ToLongTimeString()} {DateTime.Now.ToLongDateString()}");
                txtWriter.WriteLine("  :");
                txtWriter.WriteLine($"  :{logMensagem}");
                txtWriter.WriteLine("------------------------------------");
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
    }
}
