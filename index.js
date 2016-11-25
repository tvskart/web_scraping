var request = require("request"),
  cheerio = require("cheerio"),
  config = require('./config'),
  aws = require('aws-sdk'),
  _ = require('lodash'),
  url_themes = "http://www.allmovie.com/themes",
  url_root = 'http://www.allmovie.com';

var async = require('async');
var express  = require('express');
var app = express();
var port = 8080;
app.set('port', process.env.PORT || port);

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
          console.log("We’ve encountered an error: " + error);
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
      keywords = characteristics.find('.keywords .charactList').text().split(',').map((k) => {
          return k.replace(/(\r\n|\n|\r)/gm,"").trim();
      });
      var synopsis = $('section.synopsis').find('.text').text();
      synopsis = synopsis.replace(/(\r\n|\n|\r)/gm,"").trim();
      cb(null, {moods, themes, keywords, synopsis});
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
      var {Director, Actors, Genre, Poster, imdbRating} = JSON.parse(body);
      cb(null, {
        directors: (Director) ? Director.split(',') : [],
        actors: (Actors) ? Actors.split(','): [],
        picture_url: Poster,
        rating: imdbRating,
        genres: (Genre) ? Genre.split(','): []
      });
    } else {
      console.log("We’ve encountered an error: " + error);
    }
  });
}
var parseMovie = (url_movie, movie_obj) => {
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
      console.log(schemaMovie(movie_to_publish));
      snsPublish(movie_to_publish, {arn: config.sns_arn}).then(messageId => {
        console.log(messageId);
      });
    }
  });
};
// parseMovie('http://www.allmovie.com/movie/im-not-ashamed-v658837', {title: "I'm Not Ashamed", movie_year: 2016});
// parseMovie('http://www.allmovie.com/movie/the-conjuring-2-v585192', {title: "The Conjuring 2", movie_year: 2016});
//parseMovie('http://www.allmovie.com/movie/avengers-age-of-ultron-v570172', {title: "Avengers: Age of Ultron", movie_year: 2015});

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
  return obj;
}

app.get('/start', (req, res) => {
    var movie = parseMovie('http://www.allmovie.com/movie/avengers-age-of-ultron-v570172', {title: "Avengers: Age of Ultron", movie_year: 2015});
    res.json(movie);
});

var server = app.listen(app.get('port'), () => {
    console.log('App is listening on port ', server.address().port);
})

// setTimeout(parseThemes, 10);

var getMessages = () => {
  let elasticsearch = require('elasticsearch');
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
    batchSize: 5, //not bulk
    handleMessage: function (message, done) {
      var movie = JSON.parse(JSON.parse(message['Body'])['Message']);
      console.log(movie.title, movie.movie_year);
      movie_to_index = schemaMovie(movie);
      // console.log(movie_to_index);
      var is_complete =  _.get(movie, 'details.moods') && _.get(movie, 'details.themes') && _.get(movie, 'omdb.genres') && _.get(movie, 'details.keywords')
      elastic_client.index({
          index: (is_complete.length > 0) ? config.es.index_complete : config.es.index_incomplete,
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
  let bulk_movies = [];
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