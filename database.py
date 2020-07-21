import os
import pandas as pd
import numpy as np

class DB:
    """
    Once initialized the following class variables will be available:

    full:         df with road data
    mean:         df with mean of road data
    median:       df with median of road data
    sd:           df with standard deviation (SD) of road data
    dist_sd:      df with distance from mean relative to SD of road data
    datetime:     Series of dates matching road data indices
    coordinates:  Series of coordinates matching intersections in road data
    disturbances: df with info about disturbances in the railway traffic
    """
    
    def __init__(self):
        path = os.getcwd()
        if os.getenv('TRAFFIC_DATA') is None:
            # when using activate from pyvenv.el the variable isn't set.
            # iow this is a workaround for a problem not related to this project.
            print("Setting Environment Variable TRAFFIC_DATA to /home/erik/trafficData")
            os.environ['TRAFFIC_DATA']='/home/erik/trafficData'
        DB.data_dir = os.getenv('TRAFFIC_DATA')
        df = DB.load_road_data('/aggregated_csv')
        # Fit df to whole weeks
        rows_cutoff = len(df)%672
        df = df.iloc[rows_cutoff:,:] 
        DB.full = df
        # Reshape the DataFrame into a 3D numpy array (weeks at axis 0)
        # and use it to calculate the mean and standard deviation over
        # axis 0, ignoring NaNs. -1 signifies that the size of this axis is inferred.
        reshaped = df.values.reshape(-1,672,df.shape[1])
        DB.mean = pd.DataFrame(np.nanmean(reshaped,axis=0), columns=df.columns)
        DB.median = pd.DataFrame(np.nanmedian(reshaped,axis=0), columns=df.columns)
        DB.sd = pd.DataFrame(np.nanstd(reshaped,axis=0), columns=df.columns)

        # Calculate distance from the mean in terms of standard deviation
        df = (reshaped - DB.mean.values) / DB.sd.values
        DB.dist_sd = pd.DataFrame(df.reshape(DB.full.shape), columns = DB.full.columns)

        # load datetimes matching the rows in DB.full
        os.chdir(DB.data_dir)
        fn = './dates/dates.csv'
        DB.datetimes = pd.read_csv(fn, parse_dates=[0]).iloc[rows_cutoff:,0]

        # load coordinates for the intersections
        os.chdir(DB.data_dir)
        fn = './coordinates/intersections.csv'
        DB.coordinates = pd.read_csv(fn, index_col='name')
        
        # load railway disturbances data
        DB.disturbances = DB.load_simple_data('publicTransit')

        # Reset cwd
        os.chdir(path)

    def load_simple_data(subdir):
        """ 
        Loads data from a single file in a subdirectory into a dataframe.
        Directory must contain only a single csv-file with the columns
        'starttime' and 'endtime'
        """
        os.chdir(DB.data_dir)
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
        os.chdir(DB.data_dir + subdir)
        data = { i[:-4]:pd.read_csv(i) for i in os.listdir() }
        for k,v in data.items():
            v.columns = [[k] * len(v.columns), v.columns]
        df = pd.concat([v for v in data.values()], axis=1, sort=False)
        return df
