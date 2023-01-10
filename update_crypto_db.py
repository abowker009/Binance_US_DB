from datetime import datetime, timedelta
import pandas as pd

product_list= ["BTC","ETH"]
base_currency= ["USD"]

#intervals 
binace_interval= "1m"


#IGNORE everything below this for you to play with to get your timedelta math right for the final run.

# Get UTC time for the beginning of January 1, 2019
start_date = datetime(year=2022, month=12, day=8, hour=0, minute=0, second=0, microsecond=0)


# Get current time in UTC
end_date = datetime.utcnow()

# Subtract one day from UTC time
end_date -= timedelta(days=1)
# Set time to the last minute of the previous day
end_date = end_date.replace(hour=23, minute=59, second=0, microsecond=0)
# Convert UTC time to Unix time (seconds since Jan 1, 1970)
date_range = pd.date_range(start_date, end_date,freq="300min")
