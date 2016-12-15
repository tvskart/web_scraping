var elasticsearch = require('elasticsearch'),
  config = require('../config'),
    _ = require('lodash');
const elastic_client = new elasticsearch.Client({
        hosts: [
            {
                protocol: 'https',
                host: config.es.host,
                port: 443
            }
            // ,
            // {
            //     host: 'localhost',
            //     port: 9200
            // }
        ]
});

var parseCompleteES = () => {
    // Run a search, return to client side
    var body = {
        size: 500,
        from: 0,
        query: {
            // match_all: {}
            match: {
                'genres': {
                    query: 'N/A'
                }
            }
            // bool: {
            //     "must": [
            //         {
            //             range: {
            //                 release_year: {
            //                     gte: 2015,
            //                     lte: 2018
            //                 }
            //             }
            //         }
            //     ]
            // }
        }
    };
    elastic_client.search({
        index: config.es.index_incomplete, //search all indices
        type: config.es.doc_type,
        body: body
    },function (error, response,status) {
        if (error){
            console.log("search error: "+error);
        }
        else {
            let movies = [];
            let movie_ids = [];
            console.log("--- Response ---");
            console.log(response);
            console.log("--- Hits ---");
            response.hits.hits.forEach(function(hit){
                console.log(_.get(hit._source, 'title'));
                console.log(_.get(hit._source, 'genres'));
                if (_.get(hit._source, 'genres').length) {
                    movies.push(hit._source);
                    movie_ids.push(hit._id);
                }
            });
            console.log(movies.length);
        }
    });
}

var deleteMovie = (movie_id) => {
    elastic_client.delete({  
        index: config.es.index_incomplete,
        id: movie_id,
        type: config.es.doc_type
    },function(err,resp,status) {
        console.log(resp);
    });
}

parseCompleteES();