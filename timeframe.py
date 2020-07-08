import pandas as pd
import numpy as np

class Timeframe:
    _dates = pd.read_csv("./data/dates.csv", parse_dates=[0]).iloc[:,0]
    def __init__(self, starttime, endtime):
        self.bool_mask = ((Timeframe._dates >= starttime) & (Timeframe._dates <= endtime))
        self.starttime = starttime
        self.endtime = endtime
        self.dates = list(Timeframe._dates[self.bool_mask].astype(str))
        self.week_offset = self.bool_mask.idxmax() % 672
        self.first_idx = self.bool_mask.idxmax()
        self.last_idx =  self.first_idx + self.bool_mask.sum()
        self.weeks = self.last_idx // 672 + 1

    def trim(self, df):
        if len(df) == 672:
            # The dataframe has one week of data, so duplicate it if needed before trimming
            weeks_df = df.iloc[np.tile(np.arange(672), self.weeks)]
            return weeks_df.iloc[self.first_idx:self.last_idx].reset_index(drop=True)
        else:
            return df[self.bool_mask]

    def get_dates(self):
        return self.dates
