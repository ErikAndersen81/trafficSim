from flask import Flask, escape, request, jsonify, make_response
from flask_cors import CORS
import pandas as pd
import time
from datetime import datetime, timedelta
import math
import os

app = Flask(__name__)
CORS(app)

class Intersections:
    """ This singleton class holds references to the data for intersections """
    def __init__(self):
        os.chdir("data")
        subdirs = ['mean_csv', 'median_csv', 'aggregated_csv', 'deviant_csv', 'belows_csv', 'aboves_csv']
        for subdir in subdirs:
            os.chdir(subdir)
            setattr(Intersections, subdir[:-4], { i[:-4]:pd.read_csv(i) for i in os.listdir() })
            os.chdir("..")
        setattr(Intersections, "dates", pd.read_csv("dates.csv", parse_dates=[0]).iloc[:,0])
        os.chdir("..")
        

public_transit_file = "./data/publicTransit/disturbances_maintenanceformatted.csv"
public_transit = pd.read_csv(public_transit_file, parse_dates=['starttime','endtime'])
    
@app.route('/data', methods=['POST'])
def get_data():
    jsonData = request.get_json()
    start = ""
    end = ""
    datatypes = []
    intersections = []
    disturbances = True
    outliers = True
    interval = 1
    try:
        start = pd.to_datetime(jsonData.get("starttime", "2015-01-01 00:00:00"))
        end = pd.to_datetime(jsonData.get("endtime", "2015-02-01 00:00:00"))
        interval = int((end-start).total_seconds()/(60*15))
        datatypes = jsonData.get("datatypes", [])
        intersections = jsonData.get("intersections", [])
        disturbances = bool(jsonData.get("disturbances", True))
        outliers = bool(jsonData.get("outliers", True))
    except:
        print("Exception Caught")
        return make_response(jsonify({"error":"Ooops. can't process request!"}),400)
    
    bool_mask = ((Intersections.dates >= start) & (Intersections.dates <= end))
    data = dict()
    ints_data = {intersection:dict() for intersection in intersections}
    if 'deviant' in datatypes:
        ints_data = {intersection:dict() for intersection in Intersections.__dict__['aggregated'].keys()}
    simplified = False
    if 'simplified' in datatypes:
        datatypes.remove('simplified')
        simplified = True
    dates = Intersections.dates[bool_mask]
    dates = dates.astype(str)
    data['dates'] = list(dates)
    data['maxVal'] = 0
    for datatype in datatypes:
        if datatype in {"mean", "median"}:
            week_offset = bool_mask.idxmax() % 672
            first_idx = bool_mask.idxmax()
            last_idx =  first_idx + interval
            bools =  bool_mask.iloc[first_idx:last_idx]
            bools = bools.reset_index(drop=True)
            weeks = 1
            if True in bool_mask[::672].value_counts():
                weeks = bool_mask[::672].value_counts()[True] + 1
            for k in intersections:
                df = Intersections.__dict__[datatype][k]
                df = pd.concat([df for i in range(weeks)], ignore_index=True)
                df = df.iloc[first_idx:last_idx].reset_index(drop=True)
                df = df[bools]
                if simplified:
                    df = df.sum(axis=1)
                    data['maxVal'] = max(df.max(), data['maxVal'])
                    ints_data[k][datatype] = {'summed':list(df)}
                else:
                    data['maxVal'] = max(df.max().max(), data['maxVal'])
                    cols = { col:list(df[col]) for col in df }
                    ints_data[k][datatype] = cols
        elif datatype == "deviant": # This is standard and should be sent no matter what to show circles on the map
            for k in Intersections.__dict__[datatype].keys():
                df = Intersections.__dict__[datatype][k][bool_mask]
                ints_data[k][datatype] = sum(df['0'])

                df = Intersections.__dict__['aboves'][k][bool_mask]
                ints_data[k]['aboves'] = int(df.sum().iloc[0])
                df = Intersections.__dict__['belows'][k][bool_mask]
                ints_data[k]['belows'] = int(df.sum().iloc[0])
                
                df = Intersections.__dict__['aggregated'][k][bool_mask]
                ints_data[k]['size'] = df.fillna(value=0).sum().sum()
        else:
            for k in intersections:
                df = Intersections.__dict__[datatype][k][bool_mask]
                if simplified:
                    df = df.sum(axis=1)
                    data['maxVal'] = max(df.max(), data['maxVal'])
                    ints_data[k][datatype] = {'summed':list(df)}
                else:
                    data['maxVal'] = max(df.max().max(), data['maxVal'])
                    df = df.where(pd.notnull(df), None)
                    cols = { col:list(df[col]) for col in df }
                    ints_data[k][datatype] = cols
            
    if disturbances:
        starts_in_timeframe = (public_transit.starttime >= start) & (public_transit.starttime < end)
        ends_in_timeframe = (public_transit.endtime >= start) & (public_transit.endtime < end)
        df = public_transit[starts_in_timeframe | ends_in_timeframe]
        # Calculate the offset from start in 15 minutes
        x1 = (df['starttime']-start)//pd.Timedelta(minutes=15)
        # ensure we don't have a negative offset s.t. the graph doesn't overflow to the left.
        df = df.assign(x1=x1.map(lambda x: max(x,0)))
        # offset for endtime
        x2 = (df['endtime']-start)//pd.Timedelta(minutes=15)
        # shouldn't be bigger than the interval s.t. graph doesn't overflow to the right
        df = df.assign(x2=x2.map(lambda x: min(x,interval)))
        df.loc[:,'starttime'] = df['starttime'].astype(str)
        df.loc[:,'endtime'] = df['endtime'].astype(str)
        rows = []
        for idx, row in df.iterrows():
            lst = [row.location, row.type, row.starttime, row.endtime, row.x1, row.x2]
            rows.append(lst)
        data['disturbances'] = rows
    data['intersections'] = ints_data
    data['interval'] = interval
    return jsonify(**data)

@app.route('/')
def hello():
    return f'Data is being served...'

if __name__=="__main__":
    Intersections() # load data for the intersections
    app.run(host='192.168.1.86')
