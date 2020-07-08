import os
import pandas as pd

class Intersections:
    """ This singleton class holds references to the data for intersections """
    _datatypes = dict()
    maxVal = 0
    
    def __init__(self):
        os.chdir("data")
        subdirs = ['mean_csv', 'median_csv', 'aggregated_csv', 'deviant_csv', 'belows_csv', 'aboves_csv']
        for subdir in subdirs:
            os.chdir(subdir)
            setattr(Intersections, subdir[:-4], { i[:-4]:pd.read_csv(i) for i in os.listdir() })
            Intersections._datatypes[subdir[:-4]] = { i[:-4]:pd.read_csv(i) for i in os.listdir() }
            os.chdir("..")
        setattr(Intersections, "dates", pd.read_csv("dates.csv", parse_dates=[0]).iloc[:,0])
        os.chdir("..")
        
    def get_mean(intersections, timeframe, simplified):
        mean = dict()
        for k in intersections:
            df = Intersections._datatypes['mean'][k]
            df = timeframe.populate(df)
            mean[k] = Intersections._simplify_or_not(df, simplified)
        return mean

    def get_median(intersections, timeframe, simplified):
        median = dict()
        for k in intersections:
            df = Intersections._datatypes['median'][k]
            df = timeframe.populate(df)
            median[k] = Intersections._simplify_or_not(df, simplified)
        return median

    def _simplify_or_not(df, simplified):
        if simplified:
            df = df.sum(axis=1)
            Intersections.maxVal = max(df.max(), Intersections.maxVal)
            return {'summed':list(df)}
        else:
            Intersections.maxVal = max(df.max().max(), Intersections.maxVal)
            cols = { col:list(df[col]) for col in df }
            return cols
