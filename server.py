from flask import Flask, escape, request, jsonify
from flask_cors import CORS
import pandas
import time
from datetime import date, datetime

app = Flask(__name__)
CORS(app)

intersections_raw_files=[
    "./data/cut_csv/cut_K071.csv",
    "./data/cut_csv/cut_K097.csv",
    "./data/cut_csv/cut_K124.csv",
    "./data/cut_csv/cut_K128.csv",
    "./data/cut_csv/cut_K158.csv",
    "./data/cut_csv/cut_K159.csv",
    "./data/cut_csv/cut_K184.csv",
    "./data/cut_csv/cut_K189.csv",
    "./data/cut_csv/cut_K206.csv",
    "./data/cut_csv/cut_K225.csv",
    "./data/cut_csv/cut_K270.csv",
    "./data/cut_csv/cut_K302.csv",
    "./data/cut_csv/cut_K304.csv",
    "./data/cut_csv/cut_K305.csv",
    "./data/cut_csv/cut_K402.csv",
    "./data/cut_csv/cut_K405.csv",
    "./data/cut_csv/cut_K406.csv",
    "./data/cut_csv/cut_K414.csv",
    "./data/cut_csv/cut_K424.csv",
    "./data/cut_csv/cut_K430.csv",
    "./data/cut_csv/cut_K703.csv",
    "./data/cut_csv/cut_K704.csv",
    "./data/cut_csv/cut_K707.csv",
    "./data/cut_csv/cut_K711.csv"
]

# Create a dict that maps intersection tags (e.g. 'K124') to dataframes
intersections_raw = { i[-8:-4]:pandas.read_csv(i) for i in intersections_raw_files }
rows_raw=74676

intersections_median_files = [
    "./data/median_week/K071.csv",
    "./data/median_week/K097.csv",
    "./data/median_week/K124.csv",
    "./data/median_week/K128.csv",
    "./data/median_week/K158.csv",
    "./data/median_week/K159.csv",
    "./data/median_week/K184.csv",
    "./data/median_week/K189.csv",
    "./data/median_week/K206.csv",
    "./data/median_week/K225.csv",
    "./data/median_week/K270.csv",
    "./data/median_week/K302.csv",
    "./data/median_week/K304.csv",
    "./data/median_week/K305.csv",
    "./data/median_week/K402.csv",
    "./data/median_week/K405.csv",
    "./data/median_week/K406.csv",
    "./data/median_week/K414.csv",
    "./data/median_week/K424.csv",
    "./data/median_week/K430.csv",
    "./data/median_week/K703.csv",
    "./data/median_week/K704.csv",
    "./data/median_week/K707.csv",
    "./data/median_week/K711.csv"
]

# Create a dict that maps intersection tags (e.g. 'K124') to dataframes
intersections_median = { i[-8:-4]:pandas.read_csv(i) for i in intersections_median_files }
rows_median = 672

    
def index(form):
    """ 
    Returns an index in a dataframe depending on the current time.
    Each row in the dataframe corresponds to 15 minutes in real time, 
    but the simulation skips to a new row every 4'th second.
    """
    if form == "median":
        rows = rows_median
    else:
        rows = rows_raw
    return (time.time_ns()//(4*10**9))%rows

@app.route('/')
def hello():
    return f'Data is being served...'


@app.route('/<tag>/<form>')
def get_intersection(tag,form):
    idx = index(form)
    if form == "median":
        return intersections_median[tag].iloc[idx].to_json()
    else:
        return intersections_raw[tag].iloc[idx].to_json()

@app.route('/real', methods=["POST"])
def get_real_day():
    # the data posted must be json containg the key "date".
    jsonData = request.get_json()
    if not "date" in jsonData:
        return jsonify({"data":null, "error":"date field in data not specified"})
    date = jsonData["date"]
    idx = dateToIndex(date)
    data = {key:intersection.iloc[idx,2:].to_json() for key,intersection in intersections_raw.items()}
    return jsonify({"index":idx, "date":date, "data":data})

@app.route('/median', methods=["POST"])
def get_median_day():
    # the data posted must be json containg the key "date".
    jsonData = request.get_json()
    if not "date" in jsonData:
        return jsonify({"data":null, "error":"date field in data not specified"})
    date = jsonData["date"]
    idx = dateToIndex(date) % 672
    data = {key:intersection.iloc[idx,2:].to_json() for key,intersection in intersections_median.items()}
    return jsonify({"index":idx, "date":date, "data":data})

def dateToIndex(dateString):
    # The first entry in every cut_csv file has the time stamp '2017-02-06 13:15:00'.
    startDate = datetime(2017, 2, 6, hour=13, minute=15)
    getDate = datetime.fromisoformat(dateString)
    delta = getDate-startDate
    # Each entry corresponds to 15 minutes of 60 seconds each
    idx = int(delta.total_seconds()/(60*15))
    return idx

if __name__=="__main__":
    app.run(host="0.0.0.0")
