import pandas as pd
import numpy as np

class Timeframe:
    # TODO: Fix wrong offset for mean and median
    _dates = pd.read_csv("./data/dates.csv", parse_dates=[0]).iloc[:,0]
    def __init__(self, starttime, endtime):
        self.bool_mask = ((Timeframe._dates >= starttime) & (Timeframe._dates <= endtime))
        self.starttime = starttime
        self.endtime = endtime
        self.dates = list(Timeframe._dates[self.bool_mask].astype(str))
        self.indices = len(self.dates)
        self.week_offset = self.bool_mask.idxmax() % 672
        self.first_idx = self.bool_mask.idxmax()
        self.last_idx =  self.first_idx + self.bool_mask.sum()
        self.weeks = self.last_idx // 672 + 1

    def trim(self, df):
        """ Trims the dataframe (with cleaned intersection data) given as input to current timeframe """
        if len(df) == 672:
            # The dataframe has one week of data, so duplicate it if needed before trimming
            weeks_df = df.iloc[np.tile(np.arange(672), self.weeks)]
            return weeks_df.iloc[self.first_idx:self.last_idx].reset_index(drop=True)
        else:
            return df[self.bool_mask]

    def get_dates(self):
        return self.dates

    def in_timeframe(self, df):
        """ Returns rows from original dataframe 
        where the value in 'starttime' and 'endtime' columns is within the timeframe. """
        starts_in_timeframe = (df.starttime >= self.starttime) & (df.starttime < self.endtime)
        ends_in_timeframe = (df.endtime >= self.starttime) & (df.endtime < self.endtime)
        df = df[starts_in_timeframe | ends_in_timeframe]
        return df

    def datetimes_to_idxs(self, datetimes):
        """ Converts the series of datetimes to a series of indices corresponding to the current timeframe """
        # Calculate the offset from start in 15 minutes
        idxs = (datetimes-self.starttime)//pd.Timedelta(minutes=15)
        # ensure indices are within the timeframe
        idxs=idxs.map(lambda x: max(x,0))
        idxs=idxs.map(lambda x: min(x,self.indices))
        return idxs
