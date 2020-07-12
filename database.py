import os
import pandas as pd


class DB:
    """
    Once initialized the following class variables will be available:

    full:         df with road data
    mean:         df with mean of road data
    std:          df with standard deviation of road data
    datetime:     Series of dates matching roaddata indices
    disturbances: df with info about disturbances in the railway traffic
    """
    
    def __init__(self):
        if os.getenv('TRAFFIC_DATA') is None:
            # when using activate from pyvenv.el the variable isn't set.
            # iow this is a workaround for a problem not related to this project.
            os.environ['TRAFFIC_DATA']='/home/erik/trafficData'

        df = load_road_data('/aggregated_csv')
        # Fit df to whole weeks
        rows_cutoff = len(df)%672
        df = df.iloc[rows_cutoff:,:] 
        DB.full = df
        # Reshape the DataFrame into a 3D numpy array (weeks at axis 0)
        reshaped = df.values.reshape(-1,672,df.shape[1])
        # Calculate the mean and standard deviation over axis 0, ignoring NaNs.
        DB.mean = pd.DataFrame(np.nanmean(reshaped,0), columns=df.columns)
        DB.std = pd.DataFrame(np.nanstd(reshaped,0), columns=df.columns)
        
        # load datetimes matching the rows in DB.full
        os.chdir(os.getenv('TRAFFIC_DATA'))
        fn = './dates/dates.csv'
        DB.datetimes = pd.read_csv(fn, parse_dates=[0]).iloc[rows_cutoff:,0]
        
        # load railway disturbances data
        DB.disturbances = load_simple_data('publicTransit')

def load_simple_data(subdir):
    """ 
    Loads data from a single file in a subdirectory into a dataframe.
    Directory must contain only a single csv-file with the columns
    'starttime' and 'endtime'
    """
    os.chdir(os.getenv('TRAFFIC_DATA'))
    os.chdir(subdir)
    fn = os.listdir()[0]
    df = pd.read_csv(fn, parse_dates=['starttime','endtime'])
    return df


def load_road_data(subdir):
    """
    Loads the data from all the csv files in the subdirectory into a 
    single hierarchically structured dataframe which is returned.
    The structure has two column-wise levels:
    <filename> and <original column>.
    Files must have the same amount of rows.
    """
    os.chdir(os.getenv('TRAFFIC_DATA') + subdir)
    data = { i[:-4]:pd.read_csv(i) for i in os.listdir() }
    for k,v in data.items():
        v.columns = [[k] * len(v.columns), v.columns]
    df = pd.concat([v for v in data.values()], axis=1, sort=False)
    return df
