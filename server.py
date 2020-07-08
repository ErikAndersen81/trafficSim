from flask import Flask, request, jsonify, make_response
from flask_cors import CORS
from intersections import Intersections
from timeframe import Timeframe
import pandas as pd

app = Flask(__name__)
CORS(app)


public_transit_file = "./data/publicTransit/disturbances_maintenanceformatted.csv"
public_transit = pd.read_csv(public_transit_file, parse_dates=['starttime','endtime'])

def get_json_data():
    """ Retrieves the attached json data and returns it (or default values) in a dictionary """
    jsonData = request.get_json()
    try:
        starttime = pd.to_datetime(jsonData.get("starttime", "2015-01-01 00:00:00"))
        endtime = pd.to_datetime(jsonData.get("endtime", "2015-02-01 00:00:00"))
        interval = int((endtime-starttime).total_seconds()/(60*15))
        datatypes = jsonData.get("datatypes", [])
        intersections = jsonData.get("intersections", [])
        disturbances = bool(jsonData.get("disturbances", True))
        outliers = bool(jsonData.get("outliers", True))
        return {'starttime':starttime,
                'endtime':endtime,
                'interval':interval,
                'datatypes':datatypes,
                'intersections':intersections,
                'disturbances':disturbances,
                'outliers':outliers
                }
    except:
        print("Exception Caught")
        return False
    
@app.route('/data', methods=['POST'])
def get_data():
    Intersections.maxVal = 0
    json_data = get_json_data()
    timeframe = Timeframe(json_data['starttime'], json_data['endtime'])
    if not json_data:
        return make_response(jsonify({"error":"Invalid JSON data!"}),401)
    bool_mask = ((Intersections.dates >= json_data['starttime']) & (Intersections.dates <= json_data['endtime']))
    data = dict()
    ints_data = {intersection:dict() for intersection in json_data['intersections']}
    if 'deviant' in json_data['datatypes']:
        ints_data = {intersection:dict() for intersection in Intersections.__dict__['aggregated'].keys()}
    simplified = False
    if 'simplified' in json_data['datatypes']:
        json_data['datatypes'].remove('simplified')
        simplified = True
    dates = Intersections.dates[bool_mask]
    dates = dates.astype(str)
    data['dates'] = list(dates)
    data['maxVal'] = 0
    for datatype in json_data['datatypes']:
        if datatype == "mean":
            mean = Intersections.get_mean(json_data['intersections'], timeframe, simplified)
            for k,v in mean.items():
                ints_data[k]['mean']=v
        elif datatype == "median":
            median = Intersections.get_median(json_data['intersections'], timeframe, simplified)
            for k,v in median.items():
                ints_data[k]['median']=v
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
            for k in json_data['intersections']:
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
            
    if json_data['disturbances']:
        starts_in_timeframe = (public_transit.starttime >= json_data['starttime']) & (public_transit.starttime < json_data['endtime'])
        ends_in_timeframe = (public_transit.endtime >= json_data['starttime']) & (public_transit.endtime < json_data['endtime'])
        df = public_transit[starts_in_timeframe | ends_in_timeframe]
        # Calculate the offset from start in 15 minutes
        x1 = (df['starttime']-json_data['starttime'])//pd.Timedelta(minutes=15)
        # ensure we don't have a negative offset s.t. the graph doesn't overflow to the left.
        df = df.assign(x1=x1.map(lambda x: max(x,0)))
        # offset for endtime
        x2 = (df['endtime']-json_data['starttime'])//pd.Timedelta(minutes=15)
        # shouldn't be bigger than the interval s.t. graph doesn't overflow to the right
        df = df.assign(x2=x2.map(lambda x: min(x,json_data['interval'])))
        df.loc[:,'starttime'] = df['starttime'].astype(str)
        df.loc[:,'endtime'] = df['endtime'].astype(str)
        rows = []
        for idx, row in df.iterrows():
            lst = [row.location, row.type, row.starttime, row.endtime, row.x1, row.x2]
            rows.append(lst)
        data['disturbances'] = rows
    data['intersections'] = ints_data
    data['interval'] = json_data['interval']
    data['maxVal'] = max(Intersections.maxVal, data['maxVal'])
    if pd.isna(data['maxVal']):
        data['maxVal'] = 0
    return jsonify(**data)

@app.route('/')
def hello():
    return f'Data is being served...'

if __name__=="__main__":
    Intersections() # load data for the intersections
    app.run()
