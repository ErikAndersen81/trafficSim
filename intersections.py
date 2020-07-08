import os
import pandas as pd


class Intersections:
    """ This singleton class holds references to the data for intersections """
    _datatypes = dict()
    _intersections = dict()
    _timeframe = None
    maxVal = 0
    
    def __init__(self):
        os.chdir("data")
        subdirs = ['mean_csv', 'median_csv', 'aggregated_csv', 'deviant_csv', 'belows_csv', 'aboves_csv']
        for subdir in subdirs:
            os.chdir(subdir)
            Intersections._datatypes[subdir[:-4]] = { i[:-4]:pd.read_csv(i) for i in os.listdir() }
            os.chdir("..")
        os.chdir("..")

    def set_timeframe(timeframe):
        Intersections._timeframe = timeframe

    def set_intersections(intersections):
        Intersections._intersections = {k:dict() for k in intersections}

    def get_intersections():
        return Intersections._intersections
        
    def calculate_mean(simplified):
        Intersections._calculate('mean', simplified)

    def calculate_median(simplified):
        Intersections._calculate('median', simplified)

    def _calculate(datatype, simplified):
        for k in Intersections._intersections:
            df = Intersections._datatypes[datatype][k]
            df = Intersections._timeframe.trim(df)
            Intersections._intersections[k][datatype] = Intersections._simplify_or_not(df, simplified)
            
    def _simplify_or_not(df, simplified):
        if simplified:
            df = df.sum(axis=1)
            Intersections.maxVal = max(df.max(), Intersections.maxVal)
            return {'summed':list(df)}
        else:
            Intersections.maxVal = max(df.max().max(), Intersections.maxVal)
            cols = { col:list(df[col]) for col in df }
            return cols


    def calculate_deviant():
        for k in Intersections._intersections.keys():
            df = Intersections._datatypes['deviant'][k]
            df = Intersections._timeframe.trim(df)
            Intersections._intersections[k]['deviant'] = sum(df['0'])

            df = Intersections._datatypes['aboves'][k]
            df = Intersections._timeframe.trim(df)
            Intersections._intersections[k]['aboves'] = int(df.sum().iloc[0])
            df = Intersections._datatypes['belows'][k]
            df = Intersections._timeframe.trim(df)
            Intersections._intersections[k]['belows'] = int(df.sum().iloc[0])
                
            df = Intersections._datatypes['aggregated'][k]
            df = Intersections._timeframe.trim(df)
            Intersections._intersections[k]['size'] = df.fillna(value=0).sum().sum()

    def calculate_aggregated(simplified):
        for k in Intersections._intersections:
            df = Intersections._datatypes['aggregated'][k]
            df = Intersections._timeframe.trim(df)
            if simplified:
                df = df.sum(axis=1)
                Intersections.maxVal = max(df.max(), Intersections.maxVal)
                Intersections._intersections[k]['aggregated'] = {'summed':list(df)}
            else:
                Intersections.maxVal = max(df.max().max(), Intersections.maxVal)
                df = df.where(pd.notnull(df), None)
                cols = { col:list(df[col]) for col in df }
                Intersections._intersections[k]['aggregated'] = cols
