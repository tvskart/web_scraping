var ElasticsearchCSV = require('elasticsearch-csv');
var config = require(__dirname+'/../config');
// create an instance of the importer with options 
var esCSV = new ElasticsearchCSV({
    es: { index: config.es.index_incomplete, type: config.es.doc_type, host: '127.0.0.1:9200' },
    csv: { filePath: __dirname+'/backup1.csv', headers: true }
});
 
esCSV.export()
    .then(function (response) {
        // Elasticsearch response for the bulk insert 
        console.log(response);
    }, function (err) {
        console.log(err);
        // throw error 
        throw err;
    });