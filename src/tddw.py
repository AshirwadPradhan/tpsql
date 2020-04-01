from flask import Flask, request, abort, json
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import os

app = Flask(__name__)
sc = SparkContext()
sqlContext = SQLContext(sc)

@app.route('/')
def index():
    return "Hello, World!"

@app.route('/runq')
def run_query(methods=['POST']):
    if request.method == 'POST':
        if not request.json or not 'query' in request.json or not 'table' in request.json:
            abort(400)
        query = request.json['query']
        dbname = request.json['dbname']
        table = request.json['table']

        #load the table in the db
        tpath = os.path.join('db', dbname, table)
        df = sqlContext.read.load(tpath+'.csv', format='csv', inferSchema='true', header='true')
        
        #run SQL query here
        df.registerTempTable(table)
        df = sqlContext.sql(query)  

        #save in the partial output path
        toutpath = os.path.join('tmp','part-'+dbname+'-'+table+'.csv')
        out_csv = df.write.csv(toutpath)

        return json.dumps(True), 202

if __name__ == '__main__':
    app.run(debug=True)