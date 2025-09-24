import os

from flask import Flask, jsonify
import secrets

from influxdb_client import InfluxDBClient
from influxdb_client.client.flux_table import TableList

app = Flask(__name__)

app.secret_key = secrets.token_hex()
app.config['INFLUX_HOST'] = "104.53.51.51"
app.config['INFLUX_PORT'] = 8086
app.config['INFLUX_BUCKET'] = "Aircraft"
app.config['INFLUX_ORG'] = "Brian Still"
app.config['INFLUX_AUTH_TOKEN'] = "kwSpQ_8q-6cwAHgquFLhp6URqaq7134ROpHEUHMDLulH49GmU1OKdS2vXb0vB7VvdxZikGp_0RiGPc7Rk9kgrw=="




# Add other InfluxDB connection details as needed (e.g., database, username, password, token for InfluxDB 2.x)

def getInfluxClient() -> InfluxDBClient:
    url = f"http://{app.config['INFLUX_HOST']}:{app.config['INFLUX_PORT']}"
    return InfluxDBClient(url=url,token=app.config['INFLUX_AUTH_TOKEN'],org=app.config['INFLUX_ORG'])


def runQuery(q: str) -> TableList:

    client = getInfluxClient()
    qryApi = client.query_api()

    try:
        results = qryApi.query(query=q, org=app.config['INFLUX_ORG'])
        return results
    finally:
        client.close()


@app.route('/v1/aircraft/list', methods=['GET'])
def getAircraftList():

    # Flux query to get all tag keys for a specific measurement
    # schema.measurementTagKeys is the standard way to do this.
    flux_query = f'''
        import "influxdata/influxdb/schema"
        schema.measurements(bucket: "{app.config['INFLUX_BUCKET']}")
        '''

    try:
        results = runQuery(flux_query)
        values = []
        # The query returns a table with a single column '_value'.
        for table in results:
            for record in table.records:
                values.append(record.get_value())

        return jsonify(values)

    except Exception as e:
        app.logger.error(f"Error querying InfluxDB: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/v1/aircraft/status/<string:id>', methods=['GET'])
def getAircraftStatus(id:str) :


    # Flux query to get all tag keys for a specific measurement
    # schema.measurementTagKeys is the standard way to do this.
    flux_query = f'''
        from (bucket: "{app.config['INFLUX_BUCKET']}")
            |> range(start: -1d) 
            |> filter(fn: (r) => r._measurement == "{id}")
            |> last()
        '''

    try:
        results = runQuery(flux_query)
        values = {}
        # The query returns a table with a single column '_value'.
        for table in results:
            for record in table.records:
                values[record.get_field()] = record.get_value()

        return jsonify(values)

    except Exception as e:
        app.logger.error(f"Error querying InfluxDB: {e}")
        return jsonify({"error": str(e)}), 500


    return jsonify({'name': id})

if __name__ == '__main__':
    app.run(debug=True)
