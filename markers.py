import os
import pandas as pd


class Markers:
    _intersections = dict()
    _timeframe = None
    maxVal = 0
    
    def __init__(self):
        os.chdir("data/coordinates")
        Markers._intersections = pd.read_csv('intersections.csv')
        os.chdir("../..")
        print(os.getcwd())
