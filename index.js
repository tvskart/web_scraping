var request = require("request"),
  cheerio = require("cheerio"),
  config = require('./config'),
  aws = require('aws-sdk'),
  _ = require('lodash'),
  url_themes = "http://www.allmovie.com/themes",
  url_root = 'http://www.allmovie.com';

var async = require('async');
var express  = require('express');
var bodyParser = require('body-parser');

var app = express();
var port = 8080;
app.set('port', process.env.PORT || port);
app.use(bodyParser.urlencoded({extended: false})); //just trying to see if it fixes anything..

aws.config.update(config.aws);
var snsPublish = require('aws-sns-publish');

function parseThemes() {
  request(url_themes, function (error, response, body) {
    if (!error) {
      var $ = cheerio.load(body),
        themes = $(".all-themes-list .themes a");

      themes.each(function(i, elem) {
        theme = $(this).text();
        href = $(this).attr('href');
        url_theme = url_root + href + '/releaseyear-desc/';
        // console.log("theme found - ", theme, url_theme);
        //TODO: save all the themes somewhere
        parseTheme(url_theme, 1);
      });
    } else {
      console.log("We’ve encountered an error: " + error);
    }
  });
}

function parseTheme(url_theme, page) {
  console.log('theme parsing', url_theme, page);
    url_next = url_theme + page;
    request(url_next, (err, res, body) => {
      if (!err) {
          var $ = cheerio.load(body),
            movies = $(".movie-highlights .movie");

          if (movies.length > 0) {
            movies.each(function(i, elem) {
              title = $(this).find('p.title').text();
              title = title.replace(/(\r\n|\n|\r)/gm,"");
              title = title.trim();
              href= $(this).find('p.title a').attr('href');
              movie_year = $(this).find('p.movie-year').text();
              movie_year = movie_year.replace(/(\r\n|\n|\r)/gm,"");
              movie_year = movie_year.trim();
              
              url_movie = url_root + href;
              //todo: maybe simplify no need of title and year passing...
              parseMovie(url_movie, {title, movie_year});
            });
            //next page of theme
            page+=1;
            parseTheme(url_theme, page);
          } else {
            //done with this theme
            console.log('End of theme!!', url_theme);
          }
        } else {
          console.log("We’ve encountered an error: " + err);
        }        
    });
}
// parseTheme('http://www.allmovie.com/characteristic/theme/ghosts-d1664/releaseyear-desc/', 1);

var parseMovieDetails = (url_movie, cb) => {
  request(url_movie, function (error, response, body) {
    if (!error && body) {
      var $ = cheerio.load(body),
        characteristics = $("section.characteristics");

      var moods = [],
        themes = [],
        keywords = [];

      characteristics.find('.moods .charactList a').each((i, el) => {
        moods[i] = $(el).text();
      });
      characteristics.find('.themes .charactList a').each((i, el) => {
        themes[i] = $(el).text();
      });
      //resolve empty string in keywords
      if (characteristics.find('.keywords .charactList').text().trim()) {
          keywords = characteristics.find('.keywords .charactList').text().split(',').map((k) => {
              return k.replace(/(\r\n|\n|\r)/gm,"").trim();
          });
      }

      var synopsis = $('section.synopsis').find('.text').text().trim();
      synopsis = synopsis.replace(/(\r\n|\n|\r)/gm,"");
      synopsis = synopsis.trim();

      var movie_year = $('h2.movie-title span').text();
      movie_year = movie_year.replace('(','');
      movie_year = movie_year.replace(')','');

      var title = $('h2.movie-title').text().trim();
      title = title.replace(movie_year, '');
      title.replace(/(\r\n|\n|\r)/gm,"");
      cb(null, {moods, themes, keywords, synopsis, title, movie_year});
    } else {
      console.log("We’ve encountered an error: " + error);
    }
  });
};

var parseMovieReview = (url_movie, cb) => {
  var url_movie_review = url_movie + '/review';
  request(url_movie_review, function (error, response, body) {
    if (!error && body) {
      var $ = cheerio.load(body),
        review_container = $("section.review div.text");
      
      review_texts = review_container.find('p').map(function(i, elem) {
        var p = $(elem).text();
        return p;
      });
      var review = '';
      review_texts.each((i, elem) => {
        review += elem;
      });

      cb(null, review);
    } else {
      console.log("We’ve encountered an error: " + error);
    }
  });
};
var parseOMDB = (movie_obj, cb) => {
  var {title, movie_year} = movie_obj;
  var url_root = 'http://www.omdbapi.com/';
  var url = url_root + '?t=' + title + '&y='+ movie_year + '&plot=full&r=json';

  request(url, function (error, response, body) {
    if (!error && body) {
      //imdb available, etc.
      try {
          var {Director, Actors, Genre, Poster, imdbRating} = JSON.parse(body);
          // resolve spaces in key values
          cb(null, {
            directors: (Director) ? Director.split(',').map((s) => {return s.trim();}) : [],
            actors: (Actors) ? Actors.split(',').map((s) => {return s.trim();}) : [],
            picture_url: Poster,
            rating: imdbRating,
            genres: (Genre) ? Genre.split(',').map((s) => {return s.trim();}) : []
          });
      } catch(err) {
        console.log("We’ve encountered parsing omdb error?: " + err); //couldnt parse, body not proper json
      }

    } else {
      console.log("We’ve encountered an error: " + error);
    }
  });
}
var parseMovie = (url_movie, movie_obj, final_cb) => {
  async.parallel({
    review: (cb) => {
      return parseMovieReview(url_movie, cb)
    },
    details: (cb) => {
      return parseMovieDetails(url_movie, cb)
    },
    omdb: (cb) => {
      return parseOMDB(movie_obj, cb)
    }
    // dummy: (cb) => {
    //   request('https://www.google.com/', (e, r, b) => {
    //     cb(null, 'google home page');
    //   });
    // }
  }, (err, results) => {
    //both Async functions executed
    if (!err && results) {
      var movie_to_publish = _.merge(results, movie_obj);
      // console.log(schemaMovie(movie_to_publish));
      // console.log(movie_to_publish.title, movie_to_publish.movie_year);
      snsPublish(movie_to_publish, {arn: config.sns_arn}).then(messageId => {
        console.log(messageId);
        if (final_cb) final_cb(movie_to_publish);
      });
    }
  });
};
// parseMovie('http://www.allmovie.com/movie/open-city-v430223', {title: "Open City", movie_year: 2008});
// parseMovie('http://www.allmovie.com/movie/the-conjuring-2-v585192', {title: "The Conjuring 2", movie_year: 2016});
// parseMovie('http://www.allmovie.com/movie/avengers-age-of-ultron-v570172', {title: "Avengers: Age of Ultron", movie_year: 2015});

var schemaMovie = (movie) => {
  var title = _.get(movie, 'title');
  title = title.replace(/(\r\n|\n|\r)/gm,"");
  title = title.trim();
  var year = _.get(movie, 'movie_year') + '';
  year = year.replace(/(\r\n|\n|\r)/gm,"");
  year = year.trim();
  console.log(title, year);
  var obj = {
    // id:
    title: title,
    // urls: [ { video: ‘s3 url’, youtube: ‘youtube url’} …  ] //array of url objects. Each obj has s3 video url and youtube url which will have youtube id. For now just one obj stored.
    actors: _.get(movie, 'omdb.actors'),
    directors: _.get(movie, 'omdb.directors'),
    genres: _.get(movie, 'omdb.genres'),
    picture_url: _.get(movie, 'omdb.picture_url'),
    release_year: year,
    key: title.replace(/\s+/g, '').toLowerCase() + '-' + year,//IMPORTANT: title-releaseyear, so we can search for movie based on unique key known from title and year. Lowercase title, remove spaces
    rating: _.get(movie, 'omdb.rating'),
    synopsis: _.get(movie, 'details.synopsis'),
    keywords: _.get(movie, 'details.keywords'),
    themes: _.get(movie, 'details.themes'),
    moods: _.get(movie, 'details.moods'),
    review: _.get(movie, 'review')   
  }

  //unfortunate changes need to be done here
  if (_.get(obj, 'keywords')[0] == "") obj.keywords = [];
  if (_.get(obj, 'actors')) obj.actors = _.get(obj, 'actors').map((s) => {return s.trim();});
  if (_.get(obj, 'directors')) obj.directors = _.get(obj, 'directors').map((s) => {return s.trim();});
  if (_.get(obj, 'genres')) obj.genres = _.get(obj, 'genres').map((s) => {return s.trim();});  
  
  return obj;
}

//Upload movie to SNS, processing
app.get('/upload', (req, res) => {
    var movie_url = req.query.movie_url || 'http://www.allmovie.com/movie/avengers-age-of-ultron-v570172';
    var title = req.query.title || 'Avengers: Age of Ultron';
    var movie_year = req.query.movie_year || '';
    parseMovie(movie_url, {title, movie_year}, (movie) => {
      res.json(movie);
    });
});

//http://localhost:8080/search?title=Independence%20Day
app.get('/search', (req, res) => {
    var title = req.query.title || 'Avengers: Age of Ultron';
    var movie_year = req.query.movie_year || '';
    console.log(title, movie_year);
    // Run a search, return to client side
    var search_query = '';
    if (title !== '') { //default
        search_query += "title:'" + title + "'";
        // search_query += (movie_year) ? "&release_year:'" + movie_year + "'" : "";
    }
    console.log('search query', search_query);
    var elasticsearch = require('elasticsearch');
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
    elastic_client.search({
        index: '_all', //search all indices
        type: config.es.doc_type,
        q: search_query
    },function (error, response,status) {
        if (error){
            console.log("search error: "+error);
        }
        else {
            let movies = [];
            console.log("--- Response ---");
            console.log(response);
            console.log("--- Hits ---");
            response.hits.hits.forEach(function(hit){
                movies.push(hit._source);
            });
            res.json(movies);
        }
    });
});

var server = app.listen(app.get('port'), () => {
    console.log('App is listening on port ', server.address().port);
})

// setTimeout(parseThemes, 10);

var getMessages = () => {
  var elasticsearch = require('elasticsearch');
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

  // elastic_client.indices.create({
  //     index: config.es.index_complete
  // },(err, resp, status) => {
  //     if (err) {
  //         console.log(err);
  //     }
  // });

  var Consumer = require('sqs-consumer');
  var consume_movie=Consumer.create({
    queueUrl: config.sqs_url,
    batchSize: 10, //not bulk
    handleMessage: function (message, done) {
      var movie = JSON.parse(JSON.parse(message['Body'])['Message']);
      console.log(movie.title, movie.movie_year);
      movie_to_index = schemaMovie(movie);
      // console.log(movie_to_index);
      var is_complete =  _.get(movie_to_index, 'moods').length && _.get(movie_to_index, 'themes').length && _.get(movie_to_index, 'genres').length && _.get(movie_to_index, 'keywords').length;
      elastic_client.index({
          index: (is_complete > 0) ? config.es.index_complete : config.es.index_incomplete,
          type: config.es.doc_type,
          body: movie_to_index
      }).then(function (result) {
          console.log(result.created);
          done();
      }).catch(console.log);
    }
  });
  consume_movie.on('error', function (err) {
    console.log(err.message);
  });
  consume_movie.start();  
}
// getMessages();

var upload2ESBulk = (movies) => {
  var bulk_movies = [];
  movies.forEach((m) => {
      if (true) {
          console.log(m);
          bulk_movies.push(
              { index: {_index: config.es.index, _type: config.es.doc_type} },
              m
          );
      }
  });
  elastic_client.bulk({
      maxRetries: 5,
      index: config.es.index,
      type: config.es.doc_type,
      body: bulk_movies
  }, function(err,resp,status) {
      if (err) {
          console.log(err);
      }
      else {
          console.log('this many docs added - ',resp.items.length);  
      }
  });
}