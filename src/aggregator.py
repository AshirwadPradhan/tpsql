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

@app.route('/runagg', methods=['POST'])
def run_query():
    if request.method == 'POST':
        p_path = os.path.join('src','')
        if not request.json or not 'query' in request.json or not 'table' in request.json:
            abort(400)
        query = request.json['query']
        table = request.json['table']

        df = sqlContext.read.load(p_path+'tmp', format='csv', inferSchema='true', header='true')
        
        #run SQL query here
        df.registerTempTable(table[0])
        df = sqlContext.sql(query)  

        #save in the partial output path
        toutpath = os.path.join(p_path+'out','final_q')
        out_csv = df.write.csv(toutpath)

        return json.dumps(True), 202

if __name__ == '__main__':
    app.run(port = 6000, debug=True)