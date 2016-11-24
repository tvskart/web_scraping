var request = require("request"),
  cheerio = require("cheerio"),
  config = require('./config'),
  aws = require('aws-sdk'),
  _ = require('lodash'),
  url_themes = "http://www.allmovie.com/themes",
  url_root = 'http://www.allmovie.com';

var async = require('async');

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
// parseThemes();

function parseTheme(url_theme, page) {
    url_next = url_theme + page;
    request(url_next, (err, res, body) => {
      if (!err) {
          var $ = cheerio.load(body),
            movies = $(".movie-highlights .movie");

          if (movies.length > 0) {
            movies.each(function(i, elem) {
              title = $(this).find('p.title').text();
              href= $(this).find('p.title a').attr('href');
              movie_year = $(this).find('p.movie-year').text();
              
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
    console.log(results);
    //TODO: store in queue the details
    //SQS consumer to upload into ES, simple
  });
};

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
      characteristics.find('.keywords .charactList a').map((i, el) => {
        keywords[i] = $(el).text();
      });
      var synopsis = $('section.synopsis').find('.text').text();
      // console.log('Movie ', url_movie, moods, themes, keywords, synopsis);
      // console.log(_.merge(movie_obj, {moods, themes, keywords, synopsis}));
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
  var {title, year} = movie_obj;
  var url_root = 'http://www.omdbapi.com/';
  var url = url_root + '?t=' + title + '&y='+ year + '&plot=full&r=json';

  request(url, function (error, response, body) {
    if (!error && body) {
      //imdb available, etc.
      var {Director, Actors} = JSON.parse(body);
      cb(null, {Director, Actors});
    } else {
      console.log("We’ve encountered an error: " + error);
    }
  });
}
parseMovie('http://www.allmovie.com/movie/singin-in-the-rain-v44857', {title: "Singin' in the Rain", year: 1952});