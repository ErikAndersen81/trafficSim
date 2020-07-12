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
    data = dict()
    Intersections.maxVal = 0
    json_data = get_json_data()
    if not json_data:
        return make_response(jsonify({"error":"Invalid JSON data!"}),401)
    
    timeframe = Timeframe(json_data['starttime'], json_data['endtime'])
    Intersections.set_timeframe(timeframe)
    Intersections.set_intersections(json_data['intersections'])
    
    simplified = False
    if 'simplified' in json_data['datatypes']:
        json_data['datatypes'].remove('simplified')
        simplified = True
    
    
    for datatype in json_data['datatypes']:
        if datatype == "mean":
            Intersections.calculate_mean(simplified)
        elif datatype == "median":
            Intersections.calculate_median(simplified)
        elif datatype == "deviant": 
            Intersections.calculate_deviant()
        elif datatype == 'aggregated':
            Intersections.calculate_aggregated(simplified)

    
            
    if json_data['disturbances']:
        df = timeframe.in_timeframe(public_transit)
        x1 = timeframe.datetimes_to_idxs(df['starttime'])
        x2 = timeframe.datetimes_to_idxs(df['endtime'])
        df = df.assign(x1=x1, x2=x2)
        df.loc[:,'starttime'] = df['starttime'].astype(str)
        df.loc[:,'endtime'] = df['endtime'].astype(str)
        rows = []
        for idx, row in df.iterrows():
            lst = [row.location, row.type, row.starttime, row.endtime, row.x1, row.x2]
            rows.append(lst)
        data['disturbances'] = rows
        
    data['intersections'] = Intersections.get_intersections()
    data['interval'] = json_data['interval']
    data['maxVal'] = Intersections.maxVal
    data['dates'] = timeframe.get_dates()
    
    if pd.isna(data['maxVal']):
        data['maxVal'] = 0
    return jsonify(**data)

@app.route('/')
def hello():
    return f'Data is being served...'


@app.route('/markers', methods=['POST'])
def get_markers():
    data = dict()
    Intersections.maxVal = 0
    json_data = get_json_data()


if __name__=="__main__":
    Intersections() # load data for the intersections
    app.run()
