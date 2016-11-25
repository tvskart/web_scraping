# web_scraping

// parseMovie('http://www.allmovie.com/movie/avengers-age-of-ultron-v570172', {title: "Avengers: Age of Ultron", movie_year: 2015});
use this function to log the output movie json that would be indexed to ES..

getMessages() is the function that consumes from queue and uploads to ES. comment it's execution when not being used..
parseThemes() is function to start web scraping from an intial page.. comment it out if u dont want to scrape and sns to create notifications etc.. i.e. fill up queue 
