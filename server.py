from flask import Flask, escape, request, jsonify
from flask_cors import CORS
import pandas
import time
from datetime import datetime

from filedefs import ints_median_files, ints_mean_files, \
                     ints_bounds_files, ints_filled_files

app = Flask(__name__)
CORS(app)

# Create a dicts that maps intersection tags (e.g. 'K124') to dataframes 
intersections_median = { i[-8:-4]:pandas.read_csv(i) for i in ints_median_files }
intersections_mean = { i[-8:-4]:pandas.read_csv(i) for i in ints_mean_files }
intersections_bounds = { i[-8:-4]:pandas.read_csv(i) for i in ints_bounds_files }
intersections_filled = { i[-8:-4]:pandas.read_csv(i) for i in ints_filled_files }

public_transit_file = "./data/publicTransit/disturbances_maintenanceformatted.csv"
public_transit = pandas.read_csv(public_transit_file)

@app.route('/')
def hello():
    return f'Data is being served...'

@app.route('/train', methods=["POST"])
def get_train():
    # the data posted must be json containg the key "date".
    jsonData = request.get_json()
    if not "date" in jsonData:
        return jsonify({"data":null, "error":"date field in data not specified"})
    data = getOngoingDisturbances(jsonData["date"])
    print(data)
    return jsonify({"data":data})

def getOngoingDisturbances(date):
    date = datetime.fromisoformat(date)
    result = []
    for row in public_transit.iterrows():
        start = datetime.fromisoformat(row[1]['starttime'])
        end = datetime.fromisoformat(row[1]['endtime'])
        if start < date < end:
            result.append(row[1].to_json())
    return result



@app.route('/<tag>/<form>')
def get_intersection(tag,form):
    idx = index(form)
    if form == "median":
        return intersections_median[tag].iloc[idx].to_json()
    else:
        return intersections_raw[tag].iloc[idx].to_json()

@app.route('/real24h', methods=["POST"])
def get_real_24h():
    # the data posted must be json containg the key "date".
    jsonData = request.get_json()
    if not "date" in jsonData:
        return jsonify({"data":None, "error":"date field in data not specified"})
    date = jsonData["date"]
    idx = dateToIndex(date)
    if idx >= len(intersections_filled['K071']):
        print("No data for date")
        return jsonify({"error":"No such date in data", "data":None})
    data = {key:intersection.iloc[idx:idx+672,1:].to_json() for key,intersection in intersections_filled.items()}
    return jsonify({"index":idx, "date":date, "data":data})

@app.route('/real', methods=["POST"])
def get_real():
    # the data posted must be json containg the key "date".
    jsonData = request.get_json()
    if not "date" in jsonData:
        return jsonify({"data":None, "error":"date field in data not specified"})
    date = jsonData["date"]
    idx = dateToIndex(date)
    if idx >= len(intersections_filled['K071']):
        print("No data for date")
        return jsonify({"error":"No such date in data", "data":None})
    data = {key:intersection.iloc[idx,1:].to_json() for key,intersection in intersections_filled.items()}
    return jsonify({"index":idx, "date":date, "data":data})

@app.route('/bounds', methods=["POST"])
def get_bounds():
    # the data posted must be json containg the key "date".
    jsonData = request.get_json()
    if not "date" in jsonData:
        return jsonify({"data":null, "error":"date field in data not specified"})
    date = jsonData["date"]
    idx = dateToIndex(date)
    data = {key:(intersection.iloc[idx,1:]*100).to_json() for key,intersection in intersections_bounds.items()}
    return jsonify({"index":idx, "date":date, "data":data})

@app.route('/median', methods=["POST"])
def get_median():
    # the data posted must be json containg the key "date".
    jsonData = request.get_json()
    if not "date" in jsonData:
        return jsonify({"data":null, "error":"date field in data not specified"})
    date = jsonData["date"]
    idx = dateToIndex(date) % 672
    data = {key:intersection.iloc[idx,1:].to_json() for key,intersection in intersections_median.items()}
    return jsonify({"index":idx, "date":date, "data":data})

@app.route('/mean', methods=["POST"])
def get_mean():
    # the data posted must be json containg the key "date".
    jsonData = request.get_json()
    if not "date" in jsonData:
        return jsonify({"data":null, "error":"date field in data not specified"})
    date = jsonData["date"]
    idx = dateToIndex(date) % 672
    data = {key:intersection.iloc[idx,1:].to_json() for key,intersection in intersections_mean.items()}
    return jsonify({"index":idx, "date":date, "data":data})

def dateToIndex(dateString):
    # The first entry in every filled_ file has the time stamp '2017-02-06 13:15:00'.
    startDate = datetime(2017, 2, 6, hour=13, minute=15)
    getDate = datetime.fromisoformat(dateString)
    delta = getDate-startDate
    # Each entry corresponds to 15 minutes of 60 seconds each
    idx = int(delta.total_seconds()/(60*15))
    return idx

if __name__=="__main__":
    app.run()
