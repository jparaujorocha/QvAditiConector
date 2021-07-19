using Npgsql;
using Oracle.ManagedDataAccess.Client;
using QlikView.Qvx.QvxLibrary;
using QvEventLogConnectorSimple.Util;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static QvEventLogConnectorSimple.Util.Enums.Enum;

namespace QvAditiConector.Dal
{
    public class ExternalDataSource
    {
        public DataTable _dataTableStagingArea;
        public QvxField[] _qvxFields;
        private IDataReader _dataReaderStagingArea;
        private SqlConnection _connectionSqlStaging;
        private OracleConnection _connectionOracleStaging;
        private NpgsqlConnection _connectionPostGreSqlStaging;
        private readonly ClsUtil _util;
        private string _nomeTabela;
        private string _tipoConexao;
        private string _connectionString;

        public ExternalDataSource(string tipoConexao, string connectionString, string nomeTabela)
        {
            try
            {
                _tipoConexao = tipoConexao;
                _connectionString = connectionString;
                _nomeTabela = nomeTabela;
                _util = new ClsUtil();
                _dataTableStagingArea = new DataTable(_nomeTabela);
                StartConnection();
                GetDataReader();
            }
            catch (Exception ex)
            {
                throw ex;
            }
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

        private void GetDataReader()
        {
            try
            {
                if (_tipoConexao == EnumTipoDataBase.PostGreSql.ToString())
                {
                    _connectionPostGreSqlStaging.Open();
                    string instrucaoSql = "Select * From " + _nomeTabela;

                    NpgsqlCommand comandoBuscarDadosStagingArea = new NpgsqlCommand(instrucaoSql, _connectionPostGreSqlStaging);
                    comandoBuscarDadosStagingArea.CommandType = CommandType.Text;
                    NpgsqlDataReader postGreSqlDataReader = comandoBuscarDadosStagingArea.ExecuteReader();
                    _dataReaderStagingArea = postGreSqlDataReader;
                    _qvxFields = GetFieldsConnector();
                    GetDataTable(_dataReaderStagingArea.FieldCount);
                    _connectionPostGreSqlStaging.Close();

                }

                else if (_tipoConexao == EnumTipoDataBase.Sql_Server.ToString())
                {
                    _connectionPostGreSqlStaging.Open();
                    string instrucaoSql = "Select * From " + _nomeTabela;

                    //BUSCAR DADOS STAGING AREA
                    SqlCommand comandoBuscarDadosStagingArea = new SqlCommand(instrucaoSql, _connectionSqlStaging);
                    comandoBuscarDadosStagingArea.CommandType = CommandType.Text;
                    SqlDataReader sqlDataReader = comandoBuscarDadosStagingArea.ExecuteReader();
                    _dataReaderStagingArea = sqlDataReader;
                    _qvxFields = GetFieldsConnector();
                    GetDataTable(_dataReaderStagingArea.FieldCount);
                    _connectionPostGreSqlStaging.Close();
                }
                else if (_tipoConexao == EnumTipoDataBase.Oracle.ToString())
                {
                    _connectionPostGreSqlStaging.Open();
                    string instrucaoSql = "Select * From " + _nomeTabela;

                    //BUSCAR DADOS STAGING AREA

                    OracleCommand comandoBuscarDadosStagingArea = new OracleCommand(instrucaoSql, _connectionOracleStaging);
                    comandoBuscarDadosStagingArea.CommandType = CommandType.Text;
                    OracleDataReader oracleDataReader = comandoBuscarDadosStagingArea.ExecuteReader();
                    _dataReaderStagingArea = oracleDataReader;
                    _qvxFields = GetFieldsConnector();
                    GetDataTable(_dataReaderStagingArea.FieldCount);
                    _connectionPostGreSqlStaging.Close();
                }
            }

            catch (Exception ex)
            {
                throw new Exception(ex + "GetDataReader()");
            }
        }

        private QvxField[] GetFieldsConnector()
        {
            try
            {
                QvxField[] fieldsConectorAditiStaging = new QvxField[0];
                if (_dataReaderStagingArea != null)
                {
                    int quantidadeCamposConsulta = _dataReaderStagingArea.FieldCount;
                    fieldsConectorAditiStaging = new QvxField[quantidadeCamposConsulta];

                    for (int i = 0; i < quantidadeCamposConsulta; i++)
                    {
                        string tipoDado = _dataReaderStagingArea.GetFieldType(i).FullName;
                        string nomeCampo = _dataReaderStagingArea.GetName(i);

                        var qvxField = new QvxField(nomeCampo, QvxFieldType.QVX_TEXT,
    QvxNullRepresentation.QVX_NULL_FLAG_SUPPRESS_DATA, FieldAttrType.ASCII);

                        fieldsConectorAditiStaging[i] = qvxField;

                        _dataTableStagingArea.Columns.Add(_dataReaderStagingArea.GetName(i));
                    }
                }

                return fieldsConectorAditiStaging;
            }
            catch (Exception ex)
            {
                throw new Exception(ex + "GetFieldsConnector()");
            }
        }

        private void GetDataTable(int quantidadeCamposConsulta)
        {
            try
            {
                object[] ColArray = new object[quantidadeCamposConsulta];
                while (_dataReaderStagingArea.Read())
                {
                    for (int i = 0; i < quantidadeCamposConsulta; i++)
                    {
                        ColArray[i] = _dataReaderStagingArea[_dataReaderStagingArea.GetName(i)];
                    }

                    _dataTableStagingArea.LoadDataRow(ColArray, true);
                }
            }
            catch (Exception ex)
            {
                throw new Exception(ex + "GetDataTable()");
            }
        }

    }
}
