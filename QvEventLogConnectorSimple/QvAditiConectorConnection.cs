using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Text.RegularExpressions;
using Npgsql;
using Oracle.ManagedDataAccess.Client;
using QlikView.Qvx.QvxLibrary;
using QvAditiConector.Dal;
using QvEventLogConnectorSimple.Util;
using static QvEventLogConnectorSimple.Util.Enums.Enum;

namespace QvEventLogConnectorSimple
{
    internal class QvAditiConectorConnection : QvxConnection
    {
        private string _nomeTabela;
        private string _tipoConexao;
        private string _connectionString;
        private string _parameters;
        private string[] _connectionStringParameters;
        private LogApp _logApp;
        private readonly ClsUtil _util;
        private SqlConnection _connectionSqlStaging;
        private OracleConnection _connectionOracleStaging;
        private NpgsqlConnection _connectionPostGreSqlStaging;
        private ExternalDataSource _externalDataSource;

        public QvAditiConectorConnection(QvxConnection connection)
        {
            try
            {
                _logApp = new LogApp();
                _util = new ClsUtil();
                _nomeTabela = " ";
                _tipoConexao = " ";
                _connectionString = " ";
                _parameters = " ";

                //GET PARAMETERS IF NOT NULL
                if (connection != null && connection.MParameters != null && connection.MParameters.Count > 0)
                {
                    this.MParameters = connection.MParameters;
                }

                GetParametersFromConnection();

                if (string.IsNullOrWhiteSpace(_parameters) == false)
                {
                    _connectionStringParameters = _util.RecuperaParametrosConnectionString(_parameters);
                    _tipoConexao = _connectionStringParameters[0];
                    string connectionString = GetConnectionString();

                    if (TestarConexao(connectionString))
                    {
                        _connectionString = connectionString;
                        StartConnection();
                    }

                    else
                    {
                        _logApp.CriarLog("ERRO na conexão com o banco de dados.");
                    }
                }

                else
                {
                    QvxLog.Log(QvxLogFacility.Audit, QvxLogSeverity.Error, "Init() Erro de conexão. Verifique os dados ou o servidor.");
                }

                Init();
            }
            catch (Exception ex)
            {
                _logApp = new LogApp();

                if (string.IsNullOrWhiteSpace(ex.Message) == false)
                {
                    QvxLog.Log(QvxLogFacility.Audit, QvxLogSeverity.Error, "Init() Erro: " + ex.Message);
                    _logApp.CriarLog("ERRO Constructor: " + ex.Message);
                }
                else
                {
                    QvxLog.Log(QvxLogFacility.Application, QvxLogSeverity.Notice, "Init() Erro Desconhecido");
                    _logApp.CriarLog("ERRO não identificado");
                }

            }
        }

        public override void Init()
        {
            List<QvxTable> tabelas = new List<QvxTable>();

            if (string.IsNullOrWhiteSpace(_parameters) == false)
            {
                DataTable schemaTables = _connectionPostGreSqlStaging.GetSchema("Tables");

                foreach (DataRow row in schemaTables.Rows)
                {
                    _nomeTabela = (string)row[2];

                    QvxTable dadosTable = new QvxTable();
                    dadosTable.TableName = _nomeTabela;
                    dadosTable.Fields = new ExternalDataSource(_tipoConexao, _connectionString, _nomeTabela)._qvxFields;
                    dadosTable.GetRows = GetDataRowsConnector;

                    tabelas.Add(dadosTable);
                }
            }

            MTables = new List<QvxTable>(tabelas);
        }

        private void StartConnection()
        {
            try
            {
                if (_tipoConexao == EnumTipoDataBase.PostGreSql.ToString())
                {
                    _connectionPostGreSqlStaging = new NpgsqlConnection(_connectionString);
                }

                else if (_tipoConexao == EnumTipoDataBase.Oracle.ToString())
                {
                    _connectionOracleStaging = new OracleConnection(_connectionString);

                }
                else if (_tipoConexao == EnumTipoDataBase.Sql_Server.ToString())
                {
                    _connectionSqlStaging = new SqlConnection(_connectionString);
                }
            }

            catch (Exception ex)
            {
                throw new Exception(ex + "Start Connection");
            }
        }

        private bool TestarConexao(string connectionString)
        {
            try
            {
                if (_tipoConexao == EnumTipoDataBase.PostGreSql.ToString())
                {
                    NpgsqlConnection connectionPostGreSqlStaging = new NpgsqlConnection(connectionString);
                    connectionPostGreSqlStaging.Open();
                    connectionPostGreSqlStaging.Close();
                    return true;
                }

                else if (_tipoConexao == EnumTipoDataBase.Sql_Server.ToString())
                {
                    SqlConnection connectionSqlStaging = new SqlConnection(connectionString);
                    connectionSqlStaging.Open();
                    connectionSqlStaging.Close();

                    return true;

                }
                else if (_tipoConexao == EnumTipoDataBase.Oracle.ToString())
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
        private string GetConnectionString()
        {
            try
            {
                string connectionString = " ";

                if (_tipoConexao == EnumTipoDataBase.PostGreSql.ToString())
                {
                    connectionString = "server=" + _connectionStringParameters[1] + ";port=" + _connectionStringParameters[2] + ";userid=" + _connectionStringParameters[3] + ";password=" + _connectionStringParameters[4] + ";database=ConectorAditiStaging";
                }

                else if (_tipoConexao == EnumTipoDataBase.Oracle.ToString())
                {
                    connectionString = "Data Source=ConectorAditiStaging;User ID=" + _connectionStringParameters[0] + ";Password=" + _connectionStringParameters[1] + ";Connection Timeout=1500;";
                }
                else if (_tipoConexao == EnumTipoDataBase.Sql_Server.ToString())
                {
                    connectionString = "Data Source=ConectorAditiStaging;Initial Catalog=" + _connectionStringParameters[0] + ";User ID=" + _connectionStringParameters[1] + ";Password=" + _connectionStringParameters[2] + ";";
                }

                return connectionString;
            }

            catch (Exception ex)
            {
                throw new Exception(ex + "GetConnectionString");
            }
        }

        private void GetParametersFromConnection()
        {
            try
            {
                if (this.MParameters != null && this.MParameters.Count > 0)
                {
                    this.MParameters.TryGetValue("Xtype", out _parameters);
                }
            }

            catch (Exception ex)
            {
                throw new Exception(ex + "GetParametersFromConnection()");
            }
        }

        public IEnumerable<QvxDataRow> GetDataRowsConnector()
        {
            var dadosTabela = new ExternalDataSource(_tipoConexao, _connectionString, _nomeTabela)._dataTableStagingArea;

            foreach (var item in dadosTabela.Rows)
            {
                yield return MakeEntry(item as DataRow, FindTable(_nomeTabela, MTables));
            }
        }

        public QvxDataRow MakeEntry(DataRow item, QvxTable table)
        {
            try
            {
                var row = new QvxDataRow();
                var dataTableStaging = new ExternalDataSource(_tipoConexao, _connectionString, _nomeTabela)._dataTableStagingArea;

                for (int i = 0; i < dataTableStaging.Columns.Count; i++)
                {
                    var field = table.Fields.Where(a => a.FieldName == dataTableStaging.Columns[i].ColumnName)
                           .Select(b => b).FirstOrDefault();

                    row[field] = item[field.FieldName].ToString();
                }
                return row;
            }
            catch (Exception ex)
            {
                throw new Exception(ex + "MakeEntry()");
            }
        }

        public override QvxDataTable ExtractQuery(string query, List<QvxTable> qvxTables)
        {
            try
            {
                /* Make sure to remove your quotesuffix, quoteprefix, 
                 * quotesuffixfordoublequotes, quoteprefixfordoublequotes
                 * as defined in selectdialog.js somewhere around here.
                 * 
                 * In this example it is an escaped double quote that is
                 * the quoteprefix/suffix
                 */
                query = Regex.Replace(query, "\\\"", "");

                return base.ExtractQuery(query, qvxTables);
            }
            catch (Exception ex)
            {
                _logApp = new LogApp();

                if (string.IsNullOrWhiteSpace(ex.Message) == false)
                {
                    QvxLog.Log(QvxLogFacility.Audit, QvxLogSeverity.Error, "Init() Erro: " + ex.Message);
                    _logApp.CriarLog("ERRO ExtractQuery: " + ex.Message);
                }
                else
                {
                    QvxLog.Log(QvxLogFacility.Application, QvxLogSeverity.Notice, "Init() Erro Desconhecido");
                    _logApp.CriarLog("ERRO não identificado");
                }

                throw new Exception(ex + "ExtractQuery()");
            }
        }
    }
}

