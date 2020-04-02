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

@app.route('/runq', methods=['POST'])
def run_query():
    # print('this')
    if request.method == 'POST':
        # print(request.method)
        p_path = os.path.join('src','')
        if not request.json or not 'query' in request.json or not 'table' in request.json:
            abort(400)
        query = request.json['query']
        dbname = request.json['dbname']
        table = request.json['table']
        print(query)

        #load the table in the db
        tpath = os.path.join(p_path+'db', dbname, table[0])
        df = sqlContext.read.load(tpath+'.csv', format='csv', inferSchema='true', header='true')
        df.registerTempTable(table[0])
        
        if len(table) == 2:
            #load the table in the db
            tpath = os.path.join(p_path+'db', dbname, table[1])
            df_j = sqlContext.read.load(tpath+'.csv', format='csv', inferSchema='true', header='true')
            df_j.registerTempTable(table[1])
        
        #run SQL query here
        df = sqlContext.sql(query)

        #save in the partial output path
        # toutpath = os.path.join('tmp','f.csv')
        out_csv = df.toPandas().to_csv(p_path+'tmp\db'+dbname+'.csv', index=False)

        return json.dumps(True), 202

if __name__ == '__main__':
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument('-p', '--port', required=False, help='port number')
    args = vars(ap.parse_args())

    try:
        port = args['port']
        app.run(port=int(port), debug=True)
    except ValueError:
        app.run(port=5000, debug=True)