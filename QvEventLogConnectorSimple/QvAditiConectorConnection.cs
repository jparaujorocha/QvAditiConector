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
using static QvEventLogConnectorSimple.Util.Enums.Enum;

namespace QvEventLogConnectorSimple
{
    internal class QvAditiConectorConnection : QvxConnection
    {
        private string _nomeTabela;
        private string _tipoConexao;
        private string _connectionString = " ";
        private IDataReader _dataReaderStagingArea;

        public override void Init()
        {
            QvxLog.SetLogLevels(true, true);

            QvxLog.Log(QvxLogFacility.Application, QvxLogSeverity.Notice, "Init()");

                _nomeTabela = " ";
                _tipoConexao = " ";
                _connectionString = " ";
                _dataReaderStagingArea = null;

                string connectionString = "server=127.0.0.1;Port=5432;user id=postgres; password=123456;database=ConectorAditiStaging";
                string tipoConexao = BuscarTipoConexao(connectionString);

                if (tipoConexao != "ERROR")
                {
                    _connectionString = connectionString;
                    _tipoConexao = tipoConexao;
                    SetInformationsFromStagingArea(_tipoConexao, _connectionString);
                }

            /*
            var eventLogFields = new QvxField[]
            {
                new QvxField("Category", QvxFieldType.QVX_TEXT, QvxNullRepresentation.QVX_NULL_FLAG_SUPPRESS_DATA, FieldAttrType.ASCII),
                new QvxField("EntryType", QvxFieldType.QVX_TEXT, QvxNullRepresentation.QVX_NULL_FLAG_SUPPRESS_DATA, FieldAttrType.ASCII),
                new QvxField("Message", QvxFieldType.QVX_TEXT, QvxNullRepresentation.QVX_NULL_FLAG_SUPPRESS_DATA, FieldAttrType.ASCII),
                new QvxField("CategoryNumber", QvxFieldType.QVX_TEXT, QvxNullRepresentation.QVX_NULL_FLAG_SUPPRESS_DATA, FieldAttrType.ASCII),
                new QvxField("Index", QvxFieldType.QVX_TEXT, QvxNullRepresentation.QVX_NULL_FLAG_SUPPRESS_DATA, FieldAttrType.ASCII),
                new QvxField("MachineName", QvxFieldType.QVX_TEXT, QvxNullRepresentation.QVX_NULL_FLAG_SUPPRESS_DATA, FieldAttrType.ASCII),
                new QvxField("Source", QvxFieldType.QVX_TEXT, QvxNullRepresentation.QVX_NULL_FLAG_SUPPRESS_DATA, FieldAttrType.ASCII),
                new QvxField("TimeGenerated", QvxFieldType.QVX_TEXT, QvxNullRepresentation.QVX_NULL_FLAG_SUPPRESS_DATA, FieldAttrType.ASCII)
            };

        MTables = new List<QvxTable>
            {
                new QvxTable
                    {
                        TableName = "ApplicationsEventLog",
                        GetRows = GetApplicationEvents,
                        Fields = eventLogFields
                    }
            };
                */
        }

        private IEnumerable<QvxDataRow> GetApplicationEvents()
    {
        while (_dataReaderStagingArea.Read())
        {
            yield return MakeEntry(FindTable(_nomeTabela, MTables), _dataReaderStagingArea);
        }
    }

    private QvxDataRow MakeEntry(QvxTable table, IDataReader dataReader)
    {
        var row = new QvxDataRow();

        for (int i = 0; i < dataReader.FieldCount; i++)
        {
            row[table.Fields.Where(a => a.FieldName == dataReader.GetName(i))
                .Select(b => b).FirstOrDefault()] = dataReader.GetValue(i).ToString();
        }

        return row;
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

            return base.ExtractQuery(query, qvxTables);
        }


        private void SetInformationsFromStagingArea(string tipoConexao, string connectionString)
        {
            _tipoConexao = tipoConexao;
            _connectionString = connectionString;

            GetDataStagingArea(_tipoConexao, _connectionString);
        }


        private void GetDataStagingArea(string tipoConexao, string connectionString)
        {
            try
            {
                MTables = new List<QvxTable>();
                if (tipoConexao == EnumTipoDataBase.PostGreSql.ToString())
                {
                    string stringConnectionPostGreSqlStaging = connectionString;
                    NpgsqlConnection connectionPostGreSqlStaging = new NpgsqlConnection(stringConnectionPostGreSqlStaging);

                    connectionPostGreSqlStaging.Open();

                    DataTable schemaTables = connectionPostGreSqlStaging.GetSchema("Tables");

                    connectionPostGreSqlStaging.Close();

                    foreach (DataRow row in schemaTables.Rows)
                    {
                        _nomeTabela = (string)row[2];
                        DataTable colunasTabela = connectionPostGreSqlStaging.GetSchema("Columns", new[] { connectionPostGreSqlStaging.DataSource, null, _nomeTabela });
                        _dataReaderStagingArea = GetDataReader(tipoConexao, _nomeTabela, connectionString);

                        MTables.Add(new QvxTable
                        {
                            TableName = _nomeTabela,
                            GetRows = GetDataRowsConnector,
                            Fields = GetFieldsConnector(_dataReaderStagingArea)
                        });
                    }
                }

                else if (tipoConexao == EnumTipoDataBase.Sql_Server.ToString())
                {
                    string stringConnectionStagingSql = connectionString;
                    SqlConnection connectionSqlStaging = new SqlConnection(stringConnectionStagingSql);

                    connectionSqlStaging.Open();

                    DataTable schemaTables = connectionSqlStaging.GetSchema("Tables");

                    connectionSqlStaging.Close();

                    foreach (DataRow row in schemaTables.Rows)
                    {
                        _nomeTabela = (string)row[2];
                        DataTable colunasTabela = connectionSqlStaging.GetSchema("Columns", new[] { connectionSqlStaging.DataSource, null, _nomeTabela });
                        _dataReaderStagingArea = GetDataReader(tipoConexao, _nomeTabela, connectionString);

                        MTables.Add(new QvxTable
                        {
                            TableName = _nomeTabela,
                            GetRows = GetDataRowsConnector,
                            Fields = GetFieldsConnector(_dataReaderStagingArea)
                        });
                    }
                }
                else if (tipoConexao == EnumTipoDataBase.Oracle.ToString())
                {
                    string stringConnectionStagingOracle = connectionString;
                    OracleConnection connectionOracleStaging = new OracleConnection(stringConnectionStagingOracle);

                    connectionOracleStaging.Open();

                    DataTable schemaTables = connectionOracleStaging.GetSchema("Tables");

                    connectionOracleStaging.Close();

                    foreach (DataRow row in schemaTables.Rows)
                    {
                        _nomeTabela = (string)row[2];
                        DataTable colunasTabela = connectionOracleStaging.GetSchema("Columns", new[] { connectionOracleStaging.DataSource, null, _nomeTabela });
                        _dataReaderStagingArea = GetDataReader(tipoConexao, _nomeTabela, connectionString);

                        MTables.Add(new QvxTable
                        {
                            TableName = _nomeTabela,
                            GetRows = GetDataRowsConnector,
                            Fields = GetFieldsConnector(_dataReaderStagingArea)
                        });
                    }
                }
            }
            catch (Exception ex)
            {
                if (ex != null && string.IsNullOrWhiteSpace(ex.Message) == false)
                {
                    QvxLog.Log(QvxLogFacility.Application, QvxLogSeverity.Notice, ex.Message);
                }
                else
                {
                    QvxLog.Log(QvxLogFacility.Application, QvxLogSeverity.Notice, "Erro na busca de dados e inserção na aplicação. Verificar Classe QvAditiconnection");
                }
            }
        }

        public string RecuperaConnectionString(Dictionary<string, string> connectionString)
        {
            return string.Join(",", connectionString.Select(a => a.Key + "=" + a.Value).ToArray());
        }
        private string BuscarTipoConexao(string connectionString)
        {
            if (TestarConexao(EnumTipoDataBase.Oracle.ToString(), connectionString))
            {
                return "Oracle";
            }

            else if (TestarConexao(EnumTipoDataBase.Sql_Server.ToString(), connectionString))
            {
                return "Sql_Server";

            }

            else if (TestarConexao(EnumTipoDataBase.PostGreSql.ToString(), connectionString))
            {
                return "PostGreSql";
            }
            return "ERROR";
        }

        private bool TestarConexao(string tipoConexao, string connectionString)
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
        private QvxField[] GetFieldsConnector(IDataReader dataReader)
        {
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
                }
            }

            return fieldsConectorAditiStaging;
        }

        private IEnumerable<QvxDataRow> GetDataRowsConnector()
        {
            while (_dataReaderStagingArea.Read())
            {
                yield return MakeEntry(FindTable(_nomeTabela, MTables), _dataReaderStagingArea);
            }
        }


        private IDataReader GetDataReader(string tipoConexao, string nomeTabela, string connectionString)
        {
            if (tipoConexao == EnumTipoDataBase.PostGreSql.ToString())
            {
                string instrucaoSql = "Select * From " + nomeTabela;
                NpgsqlConnection connectionPostGreSqlStaging = new NpgsqlConnection(connectionString);

                //BUSCAR DADOS STAGING AREA
                connectionPostGreSqlStaging.Open();
                NpgsqlCommand comandoBuscarDadosStagingArea = new NpgsqlCommand(instrucaoSql, connectionPostGreSqlStaging);
                comandoBuscarDadosStagingArea.CommandType = CommandType.Text;
                NpgsqlDataReader postGreSqlDataReader = comandoBuscarDadosStagingArea.ExecuteReader();

                return postGreSqlDataReader;
            }

            else if (tipoConexao == EnumTipoDataBase.Sql_Server.ToString())
            {
                string instrucaoSql = "Select * From " + nomeTabela;
                SqlConnection connectionSqlStaging = new SqlConnection(connectionString);

                //BUSCAR DADOS STAGING AREA
                connectionSqlStaging.Open();
                SqlCommand comandoBuscarDadosStagingArea = new SqlCommand(instrucaoSql, connectionSqlStaging);
                comandoBuscarDadosStagingArea.CommandType = CommandType.Text;
                SqlDataReader sqlDataReader = comandoBuscarDadosStagingArea.ExecuteReader();
                connectionSqlStaging.Close();

                return sqlDataReader;
            }
            else if (tipoConexao == EnumTipoDataBase.Oracle.ToString())
            {
                string instrucaoSql = "Select * From " + nomeTabela;
                OracleConnection connectionOracleStaging = new OracleConnection(connectionString);

                //BUSCAR DADOS STAGING AREA
                connectionOracleStaging.Open();
                OracleCommand comandoBuscarDadosStagingArea = new OracleCommand(instrucaoSql, connectionOracleStaging);
                comandoBuscarDadosStagingArea.CommandType = CommandType.Text;
                OracleDataReader oracleDataReader = comandoBuscarDadosStagingArea.ExecuteReader();
                connectionOracleStaging.Close();
                return oracleDataReader;
            }
            return null;
        }
    }
}
