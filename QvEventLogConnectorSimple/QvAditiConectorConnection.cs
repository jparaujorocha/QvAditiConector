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
using QvEventLogConnectorSimple.Util;
using static QvEventLogConnectorSimple.Util.Enums.Enum;

namespace QvEventLogConnectorSimple
{
    internal class QvAditiConectorConnection : QvxConnection
    {
        private string _nomeTabela;
        private string _tipoConexao;
        private string _connectionString;
        private IDataReader _dataReaderStagingArea;
        private string _parameters;
        private readonly ClsUtil _util;
        private DataTable _dataTableColumns;
        private string[] _connectionStringParameters;
        private LogApp _logApp;
        private SqlConnection _connectionSqlStaging;
        private OracleConnection _connectionOracleStaging;
        private NpgsqlConnection _connectionPostGreSqlStaging;

        public QvAditiConectorConnection(QvxConnection connection)
        {
            try
            {
                _logApp = new LogApp();

                if (connection != null && connection.MParameters != null && connection.MParameters.Count > 0)
                    this.MParameters = connection.MParameters;

                _util = new ClsUtil();
                _nomeTabela = " ";
                _tipoConexao = " ";
                _connectionString = " ";
                _parameters = " ";
                _dataReaderStagingArea = null;
                _dataTableColumns = new DataTable();

                GetParametersFromConnection();

                if (string.IsNullOrWhiteSpace(_parameters) == false)
                {
                    _connectionStringParameters = _util.RecuperaParametrosConnectionString(_parameters);
                    _tipoConexao = _connectionStringParameters[0];
                    string connectionString = GetConnectionString(_connectionStringParameters);

                    bool conexaoOk = TestarConexao(_tipoConexao, connectionString);

                    if (conexaoOk == true)
                    {
                        _connectionString = connectionString;

                        StartConnection();

                        Init();
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
            }
            catch (Exception ex)
            {
                _logApp = new LogApp();

                if (string.IsNullOrWhiteSpace(ex.Message) == false)
                {
                    QvxLog.Log(QvxLogFacility.Audit, QvxLogSeverity.Error, "Init() Erro: " + ex.Message);
                    _logApp.CriarLog("ERRO: " + ex.Message);
                }
                else
                {
                    QvxLog.Log(QvxLogFacility.Application, QvxLogSeverity.Notice, "Init() Erro Desconhecido");
                    _logApp.CriarLog("ERRO não identificado");
                }
            }
        }

        private void StartConnection()
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

        public override void Init()
        {
            QvxLog.SetLogLevels(true, true);

            QvxLog.Log(QvxLogFacility.Application, QvxLogSeverity.Notice, "Init()");

            GetDataStagingArea();

        }

        private void GetParametersFromConnection()
        {
            if (this.MParameters != null && this.MParameters.Count > 0)
            {
                this.MParameters.TryGetValue("Xtype", out _parameters);
            }
        }

        private string GetConnectionString(string[] parameters)
        {
            string connectionString = " ";

            if (_tipoConexao == EnumTipoDataBase.PostGreSql.ToString())
            {
                connectionString = "server=" + parameters[1] + ";port=" + parameters[2] + ";userid=" + parameters[3] + ";password=" + parameters[4] + ";database=ConectorAditiStaging";
            }

            else if (_tipoConexao == EnumTipoDataBase.Oracle.ToString())
            {
                connectionString = "Data Source=ConectorAditiStaging;User ID=" + parameters[0] + ";Password=" + parameters[1] + ";Connection Timeout=1500;";
            }
            else if (_tipoConexao == EnumTipoDataBase.Sql_Server.ToString())
            {
                connectionString = "Data Source=ConectorAditiStaging;Initial Catalog=" + parameters[0] + ";User ID=" + parameters[2] + ";Password=" + parameters[2] + ";";
            }

            return connectionString;
        }

        private QvxDataRow MakeEntry(object row, QvxTable table, DataTable dadosTabela)
        {
            _logApp.CriarLog("table 1: " + dadosTabela.TableName);

            var qvxRow = new QvxDataRow();

            for (int i = 0; i < dadosTabela.Columns.Count; i++)
            {
                var field = table.Fields.Where(a => a.FieldName == dadosTabela.Columns[i].ColumnName)
                       .Select(b => b).FirstOrDefault();


                _logApp.CriarLog("table 1: " + dadosTabela.TableName + " Field " + field.FieldName);

                qvxRow[field] = row.ToString();
            }

            return qvxRow;
        }

        public override QvxDataTable ExtractQuery(string query, List<QvxTable> qvxTables)
        {
            /* Make sure to remove your quotesuffix, quoteprefix, 
             * quotesuffixfordoublequotes, quoteprefixfordoublequotes
             * as defined in selectdialog.js somewhere around here.
             * 
             * In this example it is an escaped double quote that is
             * the quoteprefix/suffix
             */
            query = Regex.Replace(query, "\\\"", "");

            _logApp.CriarLog("QUERY TABELAS: " + query);
            _logApp.CriarLog("TABELAS: " );

            for (int i = 0; i < qvxTables.Count; i++)
            {
                _logApp.CriarLog(qvxTables[i].TableName);
            }

            return base.ExtractQuery(query, qvxTables);
        }

        private void GetDataStagingArea()
        {
            this.MTables = new List<QvxTable>();
            int contadorTabelas = 0;
            int contadorReg = 0;

            if (_tipoConexao == EnumTipoDataBase.PostGreSql.ToString())
            {
                DataTable schemaTables = _connectionPostGreSqlStaging.GetSchema("Tables");

                //MUDAR DE LOCAL connectionPostGreSqlStaging.Close();

                foreach (DataRow row in schemaTables.Rows)
                {
                    _connectionPostGreSqlStaging.Open();
                    contadorReg = 0;
                    contadorTabelas++;
                    _nomeTabela = (string)row[2];
                    _dataTableColumns = new DataTable(_nomeTabela);

                    if(_nomeTabela == "tasy_dm_ep01_empr")
                    {

                    }

                    _dataReaderStagingArea = GetDataReader(_tipoConexao, _nomeTabela, _connectionPostGreSqlStaging);

                    int quantidadeCamposConsulta = _dataReaderStagingArea.FieldCount;
                    object[] ColArray = new object[quantidadeCamposConsulta];

                    QvxField[] fieldsTabela = GetFieldsConnector(_dataReaderStagingArea);

                    _logApp.CriarLog("CRIOU OS CAMPOS");

                    while (_dataReaderStagingArea.Read())
                    {
                        contadorReg++;
                        for (int i = 0; i < quantidadeCamposConsulta; i++)
                        {
                            ColArray[i] = _dataReaderStagingArea[_dataReaderStagingArea.GetName(i)];
                        }

                        _dataTableColumns.LoadDataRow(ColArray, true);
                    }

                    _logApp.CriarLog("ROWS DTB 1: " + _dataTableColumns.Rows.Count);
                    _logApp.CriarLog("Numero REGISTROS TABELA " + _nomeTabela + ": " + contadorReg);
                    
                    this.MTables.Add(new QvxTable
                    {
                        TableName = _nomeTabela,
                        GetRows = GetDataRowsConnector,
                        Fields = fieldsTabela
                    });
                    _connectionPostGreSqlStaging.Close();
                }
            }

            else if (_tipoConexao == EnumTipoDataBase.Sql_Server.ToString())
            {
                _connectionSqlStaging.Open();

                DataTable schemaTables = _connectionSqlStaging.GetSchema("Tables");
                
                foreach (DataRow row in schemaTables.Rows)
                {
                    _nomeTabela = (string)row[2];
                    DataTable colunasTabela = _connectionSqlStaging.GetSchema("Columns", new[] { _connectionSqlStaging.DataSource, null, _nomeTabela });
                    _dataReaderStagingArea = GetDataReader(_tipoConexao, _nomeTabela, null, _connectionSqlStaging);

                    MTables.Add(new QvxTable
                    {
                        TableName = _nomeTabela,
                        GetRows = GetDataRowsConnector,
                        Fields = GetFieldsConnector(_dataReaderStagingArea)
                    });
                }

                _connectionSqlStaging.Close();
            }
            else if (_tipoConexao == EnumTipoDataBase.Oracle.ToString())
            {
                _connectionOracleStaging.Open();

                DataTable schemaTables = _connectionOracleStaging.GetSchema("Tables");

                _connectionOracleStaging.Close();

                foreach (DataRow row in schemaTables.Rows)
                {
                    _nomeTabela = (string)row[2];
                    DataTable colunasTabela = _connectionOracleStaging.GetSchema("Columns", new[] { _connectionOracleStaging.DataSource, null, _nomeTabela });
                    _dataReaderStagingArea = GetDataReader(_tipoConexao, _nomeTabela, null, null, _connectionOracleStaging);

                    MTables.Add(new QvxTable
                    {
                        TableName = _nomeTabela,
                        GetRows = GetDataRowsConnector,
                        Fields = GetFieldsConnector(_dataReaderStagingArea)
                    });
                }

                _connectionOracleStaging.Close();
            }
        }

        private bool TestarConexao(string tipoConexao, string connectionString)
        {
            try
            {
                if (tipoConexao == EnumTipoDataBase.PostGreSql.ToString())
                {
                    string stringConnectionStagingPostGreSql = connectionString;
                    NpgsqlConnection connectionPostGreSqlStaging = new NpgsqlConnection(stringConnectionStagingPostGreSql);
                    connectionPostGreSqlStaging.Open();
                    connectionPostGreSqlStaging.Close();
                    return true;
                }

                else if (tipoConexao == EnumTipoDataBase.Sql_Server.ToString())
                {
                    string stringConnectionStagingSql = connectionString;
                    SqlConnection connectionSqlStaging = new SqlConnection(stringConnectionStagingSql);
                    connectionSqlStaging.Open();
                    connectionSqlStaging.Close();

                    return true;

                }
                else if (tipoConexao == EnumTipoDataBase.Oracle.ToString())
                {
                    string stringConnectionOracleStaging = connectionString;
                    OracleConnection connectionOracleStaging = new OracleConnection(stringConnectionOracleStaging);
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
        private QvxField[] GetFieldsConnector(IDataReader dataReader)
        {
            _logApp.CriarLog("ENTROU FIELDS: ");

            QvxField[] fieldsConectorAditiStaging = new QvxField[0];
            if (dataReader != null)
            {
                int quantidadeCamposConsulta = dataReader.FieldCount;
                fieldsConectorAditiStaging = new QvxField[quantidadeCamposConsulta];

                for (int i = 0; i < quantidadeCamposConsulta; i++)
                {
                    string tipoDado = dataReader.GetFieldType(i).FullName;
                    string nomeCampo = dataReader.GetName(i);

                    var qvxField = new QvxField(nomeCampo, QvxFieldType.QVX_TEXT,
QvxNullRepresentation.QVX_NULL_FLAG_SUPPRESS_DATA, FieldAttrType.ASCII);

                    fieldsConectorAditiStaging[i] = qvxField;

                    _dataTableColumns.Columns.Add(dataReader.GetName(i));
                }
            }

            return fieldsConectorAditiStaging;
        }
        private IEnumerable<QvxDataRow> GetDataRowsConnector()
        {
            var dadosTabela = _dataTableColumns;

            _logApp.CriarLog("ENTROU GET DATA ROWS CONNECTOR: ");
            _logApp.CriarLog("GetDataRowsConnector: " + dadosTabela.Rows.Count);

            foreach (var item in dadosTabela.Rows)
            {
                yield return MakeEntry(item, FindTable(_nomeTabela, MTables), dadosTabela);
            }
        }

        private IDataReader GetDataReader(string tipoConexao, string nomeTabela, NpgsqlConnection connectionPostGreSqlStaging = null, SqlConnection connectionSqlStaging = null, OracleConnection connectionOracleStaging = null)
        {
            if (tipoConexao == EnumTipoDataBase.PostGreSql.ToString())
            {
                string instrucaoSql = "Select * From " + nomeTabela;

                NpgsqlCommand comandoBuscarDadosStagingArea = new NpgsqlCommand(instrucaoSql, connectionPostGreSqlStaging);
                comandoBuscarDadosStagingArea.CommandType = CommandType.Text;
                NpgsqlDataReader postGreSqlDataReader = comandoBuscarDadosStagingArea.ExecuteReader();

                return postGreSqlDataReader;
            }

            else if (tipoConexao == EnumTipoDataBase.Sql_Server.ToString())
            {
                string instrucaoSql = "Select * From " + nomeTabela;

                //BUSCAR DADOS STAGING AREA
                SqlCommand comandoBuscarDadosStagingArea = new SqlCommand(instrucaoSql, connectionSqlStaging);
                comandoBuscarDadosStagingArea.CommandType = CommandType.Text;
                SqlDataReader sqlDataReader = comandoBuscarDadosStagingArea.ExecuteReader();

                return sqlDataReader;
            }
            else if (tipoConexao == EnumTipoDataBase.Oracle.ToString())
            {
                string instrucaoSql = "Select * From " + nomeTabela;

                //BUSCAR DADOS STAGING AREA

                OracleCommand comandoBuscarDadosStagingArea = new OracleCommand(instrucaoSql, connectionOracleStaging);
                comandoBuscarDadosStagingArea.CommandType = CommandType.Text;
                OracleDataReader oracleDataReader = comandoBuscarDadosStagingArea.ExecuteReader();
                return oracleDataReader;
            }
            return null;
        }
    }
}
