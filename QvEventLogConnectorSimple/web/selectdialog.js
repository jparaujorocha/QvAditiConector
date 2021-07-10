define( ['qvangular'
], function ( qvangular ) {
	return ['serverside', 'standardSelectDialogService', function ( serverside, standardSelectDialogService ) {
		var eventlogDialogContentProvider = {
			getConnectionInfo: function () {
				return qvangular.promise( {
					dbusage: false,
					ownerusage: false,
					dbseparator: '.',
					ownerseparator: '.',
					specialchars: '! "$&\'()*+,-/:;<>`{}~[]',
					quotesuffix: '"',
					quoteprefix: '"',
					dbfirst: true,
					keywords: []
				} );
			},
            getDatabases: function () {
                console.log("DB")
				return serverside.sendJsonRequest( "getDatabases" ).then( function ( response ) {
					return response.qDatabases;
                });
                console.log("DB2")
			},
            getOwners: function ( /*databaseName*/) {
                console.log("OW")
                return qvangular.promise([{ qName: "" }]);
                console.log("OW2")
			},
            getTables: function (qDatabaseName, qOwnerName) {
                console.log("TB")
				return serverside.sendJsonRequest( "getTables", qDatabaseName, qOwnerName ).then( function ( response ) {
					return response.qTables;
                });
                console.log("TB2")
			},
            getFields: function (qDatabaseName, qOwnerName, qTableName) {
                console.log("FIE")
				return serverside.sendJsonRequest( "getFields", qDatabaseName, qOwnerName, qTableName ).then( function ( response ) {
					return response.qFields;
                });
                console.log("FIE2")
			},
            getPreview: function ( /*databaseName, ownerName, tableName*/) {
                console.log("PV")
                return qvangular.promise([]);

                console.log("PV2")
			}
		};

		standardSelectDialogService.showStandardDialog( eventlogDialogContentProvider, {
			precedingLoadVisible: true,
			fieldsAreSelectable: true,
			allowFieldRename: true
		});
	}];
} );



