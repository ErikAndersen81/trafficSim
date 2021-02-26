from flask import Flask, request, jsonify, make_response
from flask_cors import CORS
from database import DB
from timeframe import Timeframe
import pandas as pd
import numpy as np

app = Flask(__name__)
DB()
CORS(app)


def get_json_data():
    """ Retrieves the attached json data and returns it (or default values) in a dictionary """
    jsonData = request.get_json()
    try:
        starttime = pd.to_datetime(jsonData.get(
            "starttime", "2015-01-01 00:00:00"))
        endtime = pd.to_datetime(jsonData.get(
            "endtime", "2015-02-01 00:00:00"))
        interval = int((endtime-starttime).total_seconds()/(60*15))
        graph_options = jsonData.get("graph_options", [])
        intersections = jsonData.get("intersections", [])
        if 'all' in intersections:
            intersections = DB.full.columns
        disturbances = bool(jsonData.get("disturbances", True))
        outliers = bool(jsonData.get("outliers", True))
        return {'starttime': starttime,
                'endtime': endtime,
                'interval': interval,
                'graph_options': graph_options,
                'intersections': intersections,
                'disturbances': disturbances,
                'outliers': outliers
                }
    except Exception as e:
        print(e)
        return False


@app.route('/data', methods=['POST'])
def get_data():
    data = dict()
    data['pathData'] = dict()
    data['maxVal'] = 0
    json_data = get_json_data()
    if not json_data:
        return make_response(jsonify({"error": "Invalid JSON data!"}), 401)
    timeframe = Timeframe(json_data['starttime'], json_data['endtime'])

    aggregated = False
    if 'aggregated' in json_data['graph_options']:
        json_data['graph_options'].remove('aggregated')
        aggregated = True

    def prep_for_jsonify(df, key):
        if df.empty:
            data['pathData'][key] = dict()
        else:
            if aggregated:
                df = df.sum(axis=1, level=0, skipna=True)
                data['maxVal'] = max(data['maxVal'], df.max().max())
                df = df.where(pd.notnull(df), None)
                data['pathData'][key] = df.to_dict(orient='list')
            else:
                data['maxVal'] = max(data['maxVal'], df.max(skipna=True).max())
                # Replace NaN with None s.t. we get proper null values in the JSON once we jsonify the df.
                df = df.where(pd.notnull(df), None)
                # Build a dictionary from the multicolumn df
                data['pathData'][key] = {
                    k[0]+' '+k[1]: v for k, v in df.to_dict(orient='list').items()}

    for graph_option in json_data['graph_options']:
        if graph_option == 'mean':
            df = DB.mean.loc[:, json_data['intersections']]
            df = timeframe.trim(df)
            prep_for_jsonify(df, graph_option)
        elif graph_option == 'median':
            df = DB.median.loc[:, json_data['intersections']]
            df = timeframe.trim(df)
            prep_for_jsonify(df, graph_option)

    df = DB.full.loc[:, json_data['intersections']]
    df = timeframe.trim(df)
    prep_for_jsonify(df, 'aggregated')

    data['interval'] = json_data['interval']
    data['dates'] = timeframe.get_dates()

    if pd.isna(data['maxVal']):
        data['maxVal'] = 0
    return jsonify(**data)


@app.route('/')
def hello():
    return f'Data is being served...'


@app.route('/markers', methods=['POST'])
def get_markers():
    json_data = get_json_data()
    timeframe = Timeframe(json_data['starttime'], json_data['endtime'])
    df = timeframe.trim(DB.full)
    d_sum = df.groupby(axis=1, level=0).apply(np.nansum)
    df = timeframe.trim(DB.dist_sd)
    col_count = df.columns.get_level_values(0).value_counts()
    abs_above = (df > 3).groupby(axis=1, level=0).sum()
    pct_above = (abs_above.sum(axis=0) /
                 (df.shape[0] * col_count)).round(decimals=2)
    abs_below = (df < -3).groupby(axis=1, level=0).sum()
    pct_below = ((abs_below / col_count).apply(np.nansum) /
                 df.shape[0]).round(decimals=2)
    return jsonify({"total_passings": d_sum.to_dict(), "pct_above": pct_above.to_dict(), "pct_below": pct_below.to_dict(), "measurements": timeframe.indices})


@app.route('/coordinates')
def get_coordinates():
    return jsonify({'intersections': DB.coordinates.to_dict(orient='index')})


@app.route('/FPD')
def get_FDP():
    return "Todo: Implement FPD and FPDLOF"


@app.route('/events', methods=['POST'])
def get_events():
    json_data = get_json_data()
    df = DB.events[(json_data['starttime'] <= DB.events['starttime']) & (
        DB.events['endtime'] <= json_data['endtime'])]
    return jsonify({'events': df.to_dict(orient='index')})
