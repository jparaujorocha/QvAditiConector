using System;
using System.Data.SqlClient;
using System.Security.Authentication;
using Npgsql;
using Oracle.ManagedDataAccess.Client;
using QlikView.Qvx.QvxLibrary;
using static QvEventLogConnectorSimple.Util.Enums.Enum;

namespace QvEventLogConnectorSimple
{
    internal class QvAditiConectorServer : QvxServer
    {
        string parameters = "";
        public override QvxConnection CreateConnection()
        {
            return new QvAditiConectorConnection();
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

            /**
             * -- How to get hold of connection details? --
             *
             * Provider, username and password are always available in
             * connection.MParameters if they exist in the connection
             * stored in the QlikView Repository Service (QRS).
             *
             * If there are any other user/connector defined parameters in the
             * connection string they can be retrieved in the same way as seen
             * below
             */


            string username = "postgres", password = "123456";
            connection.MParameters.TryGetValue("Xtype", out parameters);

            var con = CreateConnection();
            con.MParameters = connection.MParameters;
            /*
            connection.MParameters.TryGetValue("provider", out provider); // Set to the name of the connector by QlikView Engine
            connection.MParameters.TryGetValue("userid", out username); // Set when creating new connection or from inside the QlikView Management Console (QMC)
            connection.MParameters.TryGetValue("senha", out password); // Same as for username
            //connection.MParameters.TryGetValue("host", out host); // Defined when calling createNewConnection in connectdialog.js
            */

            switch (method)
            {
                case "getInfo":
                    response = getInfo();
                    break;
                case "getDatabases":
                    response = getDatabases(username, password);
                    break;
                case "getTables":
                    response = getTables(username, password, connection, userParameters[0], userParameters[1]);
                    break;
                case "getFields":
                    response = getFields(username, password, connection, userParameters[0], userParameters[1], userParameters[2]);
                    break;
                case "testConnection":
                    response = testConnection(userParameters[0], userParameters[1]);
                    break;
                default:
                    response = new Info { qMessage = "Unknown command" };
                    break;
            }
            return ToJson(response);    // serializes response into JSON string
        }

        public bool verifyCredentials (string username, string password) {
            return true;
        }
        
        public string[] RecuperaParametrosConnectionString(string parametros)
        {
            string[] separadores = new string[] { "-*" };
            return parametros.Split(separadores, 0);
        }

        public QvDataContractResponse getInfo()
        {
            return new Info
            {
                qMessage = "Selecione o servidor e digite os dados informados. "
            };
        }

        public QvDataContractResponse getDatabases(string username, string password)
        {
            if (verifyCredentials(username, password))
            {
                return new QvDataContractDatabaseListResponse
                {
                    qDatabases = new Database[]
                    {
                        new Database {qName = "ConectorAditiStaging"}
                    }
                };
            }
            return new Info { qMessage = "Erro nas credenciais!" };
        }

        public QvDataContractResponse getTables(string username, string password, QvxConnection connection, string database, string owner)
        {
            if (verifyCredentials(username, password))
            {
                return new QvDataContractTableListResponse
                {
                    qTables = connection.MTables
                };
            }
            return new Info { qMessage = "Erro nas credenciais!" };
        }

        public QvDataContractResponse getFields(string username, string password, QvxConnection connection, string database, string owner, string table)
        {
            if (verifyCredentials(username, password))
            {
                var currentTable = connection.FindTable(table, connection.MTables);

                return new QvDataContractFieldListResponse
                {
                    qFields = (currentTable != null) ? currentTable.Fields : new QvxField[0]
                };
            }
            return new Info { qMessage = "Erro nas credenciais!" };
        }

        public QvDataContractResponse testConnection(string tipoConexao, string connectionString)
        {
            string message = "Conexão com erro. Confira os dados ou o servidor.";

            if (TestarConexao(tipoConexao, connectionString))
            {
                message = "Conexão realizada com sucesso!";
            }
            return new Info { qMessage = message };
        }


        public bool TestarConexao(string tipoConexao, string connectionString)
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

    }
}
