﻿define( ['qvangular',
		'text!QvEventLogConnectorSimple.webroot/connectdialog.ng.html',
		'css!QvEventLogConnectorSimple.webroot/connectdialog.css'
], function ( qvangular, template) {
	return {
		template: template,
		controller: ['$scope', 'input', function ( $scope, input ) {
			function init() {
				$scope.isEdit = input.editMode;
				$scope.id = input.instanceId;
				$scope.titleText = $scope.isEdit ? "Change QV Event Log connection" : "Add QV Event Log connection";
				$scope.saveButtonText = $scope.isEdit ? "Save changes" : "Create";

				$scope.name = "";
				$scope.username = "";
				$scope.password = "";
				$scope.provider = "QvEventLogConnectorSimple.exe"; // Connector filename
				$scope.connectionInfo = "";
				$scope.connectionSuccessful = false;
				$scope.connectionString = createCustomConnectionString($scope.provider, "host=localhost;");

				input.serverside.sendJsonRequest( "getInfo" ).then( function ( info ) {
					$scope.info = info.qMessage;
				} );

				if ( $scope.isEdit ) {
					input.serverside.getConnection( $scope.id ).then( function ( result ) {
						$scope.name = result.qName;
					} );
				}
			}


			/* Event handlers */

			$scope.onOKClicked = function () {
				if ( $scope.isEdit ) {
					var overrideCredentials = ( $scope.username !== "" && $scope.password !== "" );
					input.serverside.modifyConnection( $scope.id,
						$scope.name,
						$scope.connectionString,
						$scope.provider,
						overrideCredentials,
						$scope.username,
						$scope.password ).then( function ( result ) {
							if ( result ) {
								$scope.destroyComponent();
							}
						} );
				} else {
					input.serverside.createNewConnection( $scope.name, $scope.connectionString, $scope.username, $scope.password);
					$scope.destroyComponent();
				}
			};

			$scope.onTestConnectionClicked = function () {
				input.serverside.sendJsonRequest( "testConnection", $scope.username, $scope.password ).then( function ( info ) {
					$scope.connectionInfo = info.qMessage;
					$scope.connectionSuccessful = info.qMessage.indexOf( "OK" ) !== -1;
				} );
			};

			$scope.isOkEnabled = function () {
				return $scope.name.length > 0 && $scope.connectionSuccessful;
			};

			$scope.onEscape = $scope.onCancelClicked = function () {
				$scope.destroyComponent();
			};

			
			/* Helper functions */

			function createCustomConnectionString ( filename, connectionstring ) {
				return "CUSTOM CONNECT TO " + "\"provider=" + filename + ";" + connectionstring + "\"";
			}


			init();
		}]
	};
} );