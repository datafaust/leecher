# Yelp Scraper Library - In Progress

### Use this repo to scrape yelp api for businesses based on zip code and search term.

## Install

- download th latest `businesses.db` file from google drive
- create a `.env` and place your yelp API_KEY variable
- recommend installing anaconda and creating a specific environment
- run `pip install -r requirements.txt`
- make sure playwright is installed `playwright install`
- modify the `scraper/config.py` with your params
- run main.py

## How it works

Because scraping is very dynamic the process is done in layers as opposed to in one shot. Each layer tries to grab more and more data and refine or catch what was missed before. Data is stored in sqlite database which is a file that is constantly updated and accessible.

- `run_scraper` scrapes yelps api for base infromation and appends to db
- it tries to get website and email but often fails intead getting the yelp redirect (a link to the company website)
- once `run_scraper` is complete, `website_recovery` process the rows that dont have a website and uses `playwright` to do a heavy search on the redirect to produce a real website.
- `website_recovery` will tag db table with a last updated timestamp so that we know a recovery was attempted.
- `email_recovery` does the same for emails from websites we have

## Generating campaign lists

In order to generate lists by a category search term you can run `data_scripts/campaigns/campaign_list_generator.py` and you will split the master table into email lists with valid unique emails to push to marketing software. Validation is 95% accurate so a few bad emails may squeeze through.

## How to use my config?

FORCE_OVERWRITES are meant to be used when we have improved our scraping functions and want to see if we can squeeze out more information. Search terms and zip codes can be modified as well. Category term is the search you use, category code is codes yelp uses. 

## Future Improvements

- finish functionalizing to modularize more
- ai agent integration?