using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Security.Authentication;
using Npgsql;
using Oracle.ManagedDataAccess.Client;
using QlikView.Qvx.QvxLibrary;
using QvEventLogConnectorSimple.Util;
using static QvEventLogConnectorSimple.Util.Enums.Enum;

namespace QvEventLogConnectorSimple
{
    internal class QvAditiConectorServer : QvxServer
    {
        private string _parameters = "";
        private QvxConnection _qvxConnection;

        public override QvxConnection CreateConnection()
        {
            return new QvAditiConectorConnection(_qvxConnection);
        }

        public override string CreateConnectionString()
        {
            QvxLog.Log(QvxLogFacility.Application, QvxLogSeverity.Debug, "CreateConnectionString()");
            return "localhost";
        }


        /**
         * QlikView 12 classes
         */

        public override string HandleJsonRequest(string method, string[] userParameters, QvxConnection connection)
        {
            QvDataContractResponse response;

            _qvxConnection = connection;
            connection = CreateConnection();

            connection.MParameters.TryGetValue("Xtype", out _parameters);

            switch (method)
            {
                case "getInfo":
                    response = GetInfo();
                    break;
                case "getDatabases":
                    response = GetDatabases();
                    break;
                case "getTables":
                    response = GetTables(connection);
                    break;
                case "getFields":
                    response = GetFields(connection, userParameters[2]);
                    break;
                case "getPreview":
                    response = GetPreviewData(connection, userParameters[2]);
                    break;
                case "testConnection":
                    response = TestConnection(userParameters[0], userParameters[1]);
                    break;
                default:
                    response = new Info { qMessage = "Unknown command" };
                    break;
            }

            return ToJson(response);

        }
        private QvDataContractResponse GetPreviewData(QvxConnection connection, string table)
        {
            var currentTable = connection.FindTable(table, connection.MTables);

            PreviewRow pRow = new PreviewRow();
            PreviewResponse pResp = new PreviewResponse();

            pRow.qValues = (currentTable != null) ? currentTable.GetRows().ToList() : new List<QvxDataRow>();

            pResp.qPreview = pRow.qValues;

            LogApp logA = new LogApp();


            logA.CriarLog("Quantidade: " + currentTable.GetRows().Count());
            logA.CriarLog("Valor DATAROW: " + currentTable.GetRows().Count());
            for (int i = 0; i < pRow.qValues.Count(); i++)
            {
                logA.CriarLog(pRow.qValues[i].ToString());

            }
            logA.CriarLog("Valor DATAVALUE: " + currentTable.GetRows().Count());

            for (int i = 0; i < pRow.qValues.Count(); i++)
            {
                var field = currentTable.Fields.Where(a => a.FieldName == "cd_pf_auditor")
                       .Select(b => b).FirstOrDefault();
                logA.CriarLog(pRow.qValues[i][field].ToString());
            }

            return new PreviewResponse
            {

                qPreview = pResp.qPreview
            };
        }

        private QvDataContractResponse GetInfo()
        {
            return new Info
            {
                qMessage = "Selecione o servidor e digite os dados informados. "
            };
        }

        private QvDataContractResponse GetDatabases()
        {
            return new QvDataContractDatabaseListResponse
            {
                qDatabases = new Database[]
                {
                        new Database {qName = "ConectorAditiStaging"}
                }
            };
        }

        private QvDataContractResponse GetTables(QvxConnection connection)
        {
            return new QvDataContractTableListResponse
            {
                qTables = connection.MTables
            };
        }

        private QvDataContractResponse GetFields(QvxConnection connection, string table)
        {
            var currentTable = connection.FindTable(table, connection.MTables);

            return new QvDataContractFieldListResponse
            {
                qFields = (currentTable != null) ? currentTable.Fields : new QvxField[0]
            };
        }


        private QvDataContractResponse TestConnection(string tipoConexao, string connectionString)
        {
            string message = "Conexão com erro. Confira os dados ou o servidor.";

            if (TestarConexao(tipoConexao, connectionString))
            {
                message = "Conexão realizada com sucesso!";
            }
            return new Info { qMessage = message };
        }

        private bool TestarConexao(string tipoConexao, string connectionString)
        {

            try
            {
                if (tipoConexao == EnumTipoDataBase.PostGreSql.ToString())
                {
                    string stringConnectionPostGreSqlStaging = connectionString;
                    NpgsqlConnection connectionPostGreSqlStaging = new NpgsqlConnection(stringConnectionPostGreSqlStaging);
                    connectionPostGreSqlStaging.Open();
                    connectionPostGreSqlStaging.Close();

                    return true;
                }

                else if (tipoConexao == EnumTipoDataBase.Sql_Server.ToString())
                {
                    string stringConnectionSqlExterno = connectionString;
                    SqlConnection connectionSqlStaging = new SqlConnection(stringConnectionSqlExterno);
                    connectionSqlStaging.Open();
                    connectionSqlStaging.Close();

                    return true;

                }
                else if (tipoConexao == EnumTipoDataBase.Oracle.ToString())
                {
                    OracleConnection connectionOracleStaging = new OracleConnection(connectionString);
                    connectionOracleStaging.Open();
                    connectionOracleStaging.Close();

                    return true;
                }

                return false;
            }
            catch (Exception ex)
            {
                return false;
            }
        }

    }

    public class PreviewRow
    {
        public PreviewRow()
        {
            this.qValues = new List<QvxDataRow>();
        }

        public List<QvxDataRow> qValues { get; set; }
    }



    public class PreviewResponse : QvDataContractResponse

    {

        public PreviewResponse() : base()

        {

            this.qPreview = new List<QvxDataRow>();

        }

        public List<QvxDataRow> qPreview { get; set; }

    }
}
