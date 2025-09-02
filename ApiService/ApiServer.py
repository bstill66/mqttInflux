from flask import Flask, jsonify
import secrets

from flask_influxdb import InfluxDB

app = Flask(__name__)

app.secret_key = secrets.token_hex()
app.config['INFLUXDB_HOST'] = 'localhost'
app.config['INFLUXDB_PORT'] = 8086
# Add other InfluxDB connection details as needed (e.g., database, username, password, token for InfluxDB 2.x)

influxdb = InfluxDB(app)

@app.route('/v1/aircraft/list', methods=['GET'])
def getAircraftList():
    return jsonify(["N801DL","N404DL"])

@app.route('/v1/aircraft/status/<id>', methods=['GET'])
def getAircraftStatus(id:str) :
    return jsonify({'name': id})

if __name__ == '__main__':
    app.run(debug=True)
