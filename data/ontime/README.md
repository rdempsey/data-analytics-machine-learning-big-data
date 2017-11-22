This data set was downloaded from the U.S. Department of Transportation, Office of the
Secretary of Research on November 30, 2014 and represents flight data for the domestic
United States in April of 2014. 

The following CSV files are lookup tables:

    - airlines.csv
    - airports.csv

And provided detailed information about the reference codes in the main data set. These
files have header rows to identify their fields. 

The flights.csv contains flight statistics for April 2014 with the following fields:

- flight date     (yyyy-mm-dd)
- airline id      (lookup in airlines.csv)
- flight num
- origin          (lookup in airports.csv)
- destination     (lookup in airports.csv)
- departure time  (HHMM)
- departure delay (minutes)
- arrival time    (HHMM)
- arrival delay   (minutes)
- air time        (minutes)
- distance        (miles)

