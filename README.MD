Get Station Id
Environment Canada provides Google sheet listing stations with station name, station id, data availability start and end year and month. 
https://docs.google.com/spreadsheets/d/1CQDjEv3Hf6FRzztKlVUSGtdCnJ0IxvYZMrA35EmKfAk/edit#gid=681791018

Precipitation Data
Comes from  https://climate.weather.gc.ca/climate_data/bulk_data_e.html
Python method here uses requests to download csv files for multiple years/months for station id.

Hydrometric Flow Data
Download 'Hydrat' National Water Data Archive sqlite db which has historical flow and level data for all Canada stations

https://www.canada.ca/en/environment-climate-change/services/water-overview/quantity/monitoring/survey/data-products-services/national-archive-hydat.html

Read more about how this was used on my blog https://009co.com/?p=1165
