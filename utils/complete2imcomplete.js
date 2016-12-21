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
            match_all: {},
            // match: {
            //     'video_urls': {
            //         query: '...'
            //     }
            // },
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
        index: config.es.index_complete, //search all indices
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
                console.log(_.get(hit._source, 'urls'));
                // if (_.get(hit._source, 'genres').length) {
                    movies.push(hit);
                    movie_ids.push(hit._id);
                // }
            });
            console.log(movies.length);
            //update movie here
            console.log(movies[0]);
            movie_ids.forEach((el, i) => {
                console.log(el, i);
                setTimeout(() => {
                    // updateMovie(el, movies[i]);
                }, i*10)
            })
        }
    });
}

// var updateMovie = (movie_id, movie_hit) => {
//     //Modify hit
//     var movie_body = movie_hit._source;
//     delete movie_body['urls'];
//     delete movie_body['video_urls'];
//     console.log('updating - ', movie_id, movie_body.title);
//     if (movie_id == movie_hit._id) {
//         //index a whole doc, or update partial doc
//         elastic_client.delete({
//             index: config.es.index_complete,
//             id: movie_id,
//             type: config.es.doc_type,
//         }, function (error, response) {
//             if (error) {
//                 console.log(error)
//             } else {
//                 console.log('success', response);
//                 elastic_client.index({
//                     index: config.es.index_complete,
//                     // id: movie_id,
//                     type: config.es.doc_type,
//                     // body: {
//                     //     // put the partial document under the `doc` key
//                     //     // doc: {
//                     //     //     urls: 'karthik'
//                     //     // }
//                     //     doc: movie_body
//                     // }
//                     body: movie_body
//                 }, function (error, response) {
//                     if (error) {
//                         console.log(error)
//                     } else {
//                         console.log('success', response);
//                     }
//                 })
//             }
//         });
//     }

// }
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