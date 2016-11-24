module.exports = {
    aws: {
        'region': 'us-east-1'
    },
    sns_arn: 'arn:aws:sns:us-east-1:892410376055:movies',
    sqs_url: 'https://sqs.us-east-1.amazonaws.com/892410376055/movies',
    es: {
        host: 'search-complete-movies-t2j4rpxjydllcth3egza67bxpy.us-east-1.es.amazonaws.com',
        index_complete: 'complete_movies',
        index_incomplete: 'incomplete_movies',
        doc_type: 'movie'
    }
}