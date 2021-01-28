# trafficSim


## Running via Docker:
Use -v to point at the data that will be served, in this example it's located at /home/erik/trafficData. The Docker image exposes port 5000 so be sure to publish that, preferably to 5000 s.t. it can connect to the React server trafficViz.
> docker run -v /home/erik/trafficData/:/trafficData:ro --publish 5000:5000 --name trafficSim erikandersen81/trafficsim


## Native installation

### Installing requirements via pip 
Note that you may want to create a virtual environment to install the dependencies in and run the server.
> $ pip3 install -r requirements.txt

### Set the flask variables
As a minimum export the variables
> $ export FLASK_APP="server.py"

> $ export TRAFFIC_DATA="/mnt/sda2/home/erik/trafficData"

Make sure to replace the example directory above with the path to your local copy of the data.

You might also want to set

> FLASK_DEBUG="True"

> FLASK_ENV="development"

### Running the server
Start the server by navigating to the root directory of the application and type
> flask run
