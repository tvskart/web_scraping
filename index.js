var request = require("request"),
  cheerio = require("cheerio"),
  config = require('./config'),
  aws = require('aws-sdk'),
  _ = require('lodash'),
  url_themes = "http://www.allmovie.com/themes",
  url_root = 'http://www.allmovie.com';

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
        console.log("theme found - ", theme, url_theme);
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
              // console.log("movie found - ", title, url_movie);
              //TODO: save mapping of theme and movie-year?
              parseMovie(url_movie);
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

parseMovie = (url_movie, movie_obj) => {
  request(url_movie, function (error, response, body) {
    if (!error && body) {
      var $ = cheerio.load(body),
        characteristics = $("section.characteristics");

      var moods = [],
        themes = [],
        keywords = []
      ;
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
      console.log('Movie ', url_movie, moods, themes, keywords, synopsis);
      console.log(_.merge(movie_obj, {moods, themes, keywords, synopsis}));
      //TODO: get details from review, etc...
      //TODO: store mapping of movie to moods, themes, keywords via elastic search etc.
      // snsPublish(tweet, {arn: 'arn:aws:sns:us-west-2:012274775406:Processed_tweet'}).then(messageId => {
      //   console.log(messageId);
      // });
    } else {
      console.log("We’ve encountered an error: " + error);
    }
  });
};
parseMovie('http://www.allmovie.com/movie/singin-in-the-rain-v44857');