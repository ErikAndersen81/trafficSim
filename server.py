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
        graph_options = jsonData.get("graph_options", [])
        intersections = jsonData.get("intersections", [])
        if len(intersections) == 0:
            intersections = DB.full.columns
        bin_size = jsonData.get('bin_size', 1)
        return {'starttime': starttime,
                'endtime': endtime,
                'bin_size': bin_size,
                'graph_options': graph_options,
                'intersections': intersections,
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
                if json_data['bin_size'] > 1:
                    df = bin_df(df, json_data['bin_size'])
                data['maxVal'] = max(data['maxVal'], df.max().max())
                df = df.where(pd.notnull(df), None)
                data['pathData'][key] = df.to_dict(orient='list')
            else:
                data['maxVal'] = max(data['maxVal'], df.max(skipna=True).max())
                if json_data['bin_size'] > 1:
                    df = bin_df(df, json_data['bin_size'])
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

    data['dates'] = timeframe.get_dates()[::json_data['bin_size']]
    data['coordinates'] = DB.coordinates.to_dict('index')

    if pd.isna(data['maxVal']):
        data['maxVal'] = 0
    return jsonify(**data)


def bin_df(df, span):
    return pd.DataFrame([df.iloc[i:i+span].sum(axis=0)
                         for i in range(0, df.shape[0], span)])


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


@app.route('/distances', methods=['POST'])
def get_FDP_distances():
    json_data = get_json_data()
    if not json_data:
        return make_response(jsonify({"error": "Invalid JSON data!"}), 401)
    timeframe = Timeframe(json_data['starttime'], json_data['endtime'])
    df = DB.full.loc[:, json_data['intersections']]
    df = timeframe.trim(df)
    df = df.sum(axis=1, level=0, skipna=True)
    df = intersection_distances(df)
    df = df.where(pd.notnull(df), None)
    return jsonify({"columns": df.columns.values.tolist(), "matrix": df.values.tolist()})


@app.route('/events', methods=['POST'])
def get_events():
    json_data = get_json_data()
    df = DB.events[(json_data['starttime'] <= DB.events['starttime']) & (
        DB.events['endtime'] <= json_data['endtime'])]
    return jsonify({'events': df.to_dict(orient='index')})


def intersection_distances(df):
    """
    input: A dataframe containing a timeslice of all intersections, 
    one column pr intersection.

    returns a distance(Bhattacharyya) matrix, based on FPDs
    """
    # Normalize the data by using bins to get the relative intensity
    binned_df = get_binned_DataFrame(df, 10)
    # FPDs are then estimated
    FPDs = df_value_counts(binned_df)
    return Bhattacharyya_dists(FPDs)


def get_binned_DataFrame(df, bins):
    # Each column in the DataFrame has the function f:X -> {1,2,..., bins} applied,
    # where f divides the range [min(X);max(X)] into equally sized bins.
    # Nan values will remain NaN values.
    cuts = dict()
    for col in df.columns:
        cuts[col] = pd.to_numeric(pd.cut(df[col], bins, labels=range(bins)))
    return pd.DataFrame(cuts)


def df_value_counts(df):
    return pd.DataFrame({col: df[col].value_counts() for col in df.columns})


def Bhattacharyya_dists(df):
    """ 
    The distance matrix is calculated the way shown at https://en.wikipedia.org/wiki/Bhattacharyya_distance.
    Performed column wise.
    """
    dists = np.zeros((df.shape[1], df.shape[1]))
    var = (df.std()**2).to_numpy()
    mean = df.mean().to_numpy()
    for i in range(df.shape[1]):
        for j in range(i+1, df.shape[1]):
            A = 1/4*(np.log(1/4 * (var[i]/var[j]+var[j]/var[i] + 2)))
            B = 1/4 * ((mean[i]-mean[j])**2/(var[i]+var[j]))
            dists[i, j] = dists[j, i] = A+B
    return pd.DataFrame(dists, columns=df.columns, index=df.columns)
