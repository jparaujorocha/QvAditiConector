define(['qvangular',
    'text!QvEventLogConnectorSimple.webroot/connectdialog.ng.html',
    'css!QvEventLogConnectorSimple.webroot/connectdialog.css'
], function (qvangular, template) {
    return {
        template: template,
        controller: ['$scope', 'input', function ($scope, input) {
            function init() {
                $scope.isEdit = input.editMode;
                $scope.id = input.instanceId;
                $scope.titleText = $scope.isEdit ? "Editar conexão Conector Aditi" : "Adicionar Conexão Conector Aditi";
                $scope.saveButtonText = $scope.isEdit ? "Salvar Alterações" : "Adicionar";

                $scope.name = "";
                $scope.serverpostgresql = "";
                $scope.portpostgresql = "";
                $scope.username = "";
                $scope.password = "";
                $scope.databasepostgresql = "";
                $scope.datasourcesql = "";
                $scope.initialcatalog = "";
                $scope.datasourceoracle = "";
                $scope.provider = "QvEventLogConnectorSimple.exe"; // Connector filename
                $scope.connectionInfo = "";
                $scope.connectionSuccessful = false;

                input.serverside.sendJsonRequest("getInfo").then(function (info) {
                    $scope.info = info.qMessage;
                });

                if ($scope.isEdit) {
                    input.serverside.getConnection($scope.id).then(function (result) {
                        $scope.name = result.qName;
                    });
                }
            }


            /* Event handlers */

            $scope.onOKClicked = function () {
                if ($scope.isEdit) {
                    var overrideCredentials = ($scope.username !== "" && $scope.password !== "");
                    input.serverside.modifyConnection($scope.id,
                        $scope.name,
                        $scope.connectionString,
                        $scope.provider,
                        overrideCredentials,
                        $scope.username,
                        $scope.password).then(function (result) {
                            if (result) {
                                $scope.destroyComponent();
                            }
                        });
                } else {
                    input.serverside.createNewConnection($scope.name, $scope.connectionString, $scope.username, $scope.password);
                    $scope.destroyComponent();
                }
            };

            $scope.onTestConnectionClicked = function () {
                testConnectionString();
            };

            $scope.isOkEnabled = function () {
                return $scope.name.length > 0 && $scope.connectionSuccessful;
            };

            $scope.onEscape = $scope.onCancelClicked = function () {
                $scope.destroyComponent();
            };

            $scope.ServerChanged = function () {
                $scope.ServerStatus = $scope.ServerValue;

                if ($scope.ServerStatus == 'Oracle') {
                    $scope.isHideOracle = !$scope.isHideOracle;

                } else if ($scope.ServerStatus == 'Sql_Server') {
                    $scope.isHideSqlServer = !$scope.isHideSqlServer;

                } else if ($scope.ServerStatus == 'PostGreSql') {
                    $scope.isHidePostGreSql = !$scope.isHidePostGreSql;

                }
            };

            /* Helper functions */

            function createCustomConnectionString(filename, connectionstring) {
                return "CUSTOM CONNECT TO " + "\"provider=" + filename + ";" + connectionstring + "\"";
            }

            function testConnectionString() {
                var typeConnection = getTypeServer();
                
                if (typeConnection !== undefined) {
                    var connectionStringTeste = createConnectionStringTeste();
                    input.serverside.sendJsonRequest("testConnection", typeConnection, connectionStringTeste).then(function (info) {
                        $scope.connectionInfo = info.qMessage;
                        if (info.qMessage == "Conexão realizada com sucesso!") {
                            $scope.connectionSuccessful = true;
                        }
                        else {
                            $scope.connectionSuccessful = false;
                        }
                        if ($scope.connectionSuccessful) {
                            var connectionString = createConnectionString();
                            $scope.connectionString = createCustomConnectionString($scope.provider, connectionString);
                        }
                    });
                }
                else {
                    $scope.connectionSuccessful = false;
                }
            }


            function getTypeServer() {

                $scope.ServerStatus = $scope.ServerValue;

                if ($scope.ServerStatus == 'Oracle') {
                    return "Oracle";

                } else if ($scope.ServerStatus == 'Sql_Server') {
                    return "Sql_Server";

                } else if ($scope.ServerStatus == 'PostGreSql') {
                    return "PostGreSql";
                }
            }

            function createConnectionStringTeste() {
                var connectionStringTeste = " ";
                var typeConnection = getTypeServer();
                if (typeConnection == "Oracle") {
                    connectionStringTeste = "User ID=" + $scope.username + ";Password=" + $scope.password + ";Connection Timeout=1500";
                }
                else if (typeConnection == "Sql_Server") {
                    connectionStringTeste = "Initial Catalog=" + $scope.initialcatalog + ";User ID=" + $scope.username + ";Password=" + $scope.password + ";";
                }
                else if (typeConnection == "PostGreSql") {
                    connectionStringTeste = "host=localhost;Server=" + $scope.serverpostgresql + ";Port=" + $scope.portpostgresql + ";UserId=" + $scope.username + ";Password=" + $scope.password + ";";                    
                }
                return connectionStringTeste;
            }

            function createConnectionString() {
                var connectionString = " ";
                var typeConnection = getTypeServer();
                if (typeConnection == "Oracle") {
                    connectionString = "host=localhost;Xtype=Oracle" + "-*" + $scope.username + "-*" + $scope.password + ";";
                }
                else if (typeConnection == "Sql_Server") {
                    connectionString = "host=localhost;Xtype=Sql_Server" + "-*" + $scope.initialcatalog + "-*" + $scope.username + "-*" + $scope.password + "-*" + $scope.initialcatalog +";";

                }
                else if (typeConnection == "PostGreSql") {
                    connectionString = "host=localhost;Xtype=PostGreSql" + "-*" + $scope.serverpostgresql + "-*" + $scope.portpostgresql + "-*" + $scope.username + "-*" + $scope.password + ";";
                }
                return connectionString;
            }

            init();
        }]
    };
});