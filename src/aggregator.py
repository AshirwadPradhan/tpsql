from flask import Flask, request, abort, json
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import os
import pathlib

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
        lt = request.json['lt']

        df = sqlContext.read.load(p_path+'tmp', format='csv', inferSchema='true', header='true')
        print('Collected all the temporary outputs of Task Manager')

        #find left part and right part in case of join
        d_c = df.columns
        if lt < len(d_c):
            print('Identified join query')
            right_col = []
            for i in range(lt, len(d_c)):
                right_col.append(d_c[i])
            left_col = []
            for i in range(0, lt):
                left_col.append(d_c[i])
            print('Identified the left table coulmns and right table columns')
            df_j = df.select(right_col)
            print('Loaded Right table columns')
            df_j.registerTempTable(table[2])
        # print(right_col)
        # print(left_col)
        df.registerTempTable(table[0])
        #if table == 4
        if len(table) == 4:
            df.registerTempTable(table[1])
            df = df.select(left_col)
            print('Loaded Left table columns')
        #add table[1] as registertemptable
        #and df = df.select(left_col)
        # print(df.columns)
        # print(df_j.columns)
        # print(query)
        # print(table)
        #run SQL query here
        out = sqlContext.sql(query)
        print('Query Executed Successfully')  

        #save in the partial output path
        toutpath = os.path.join(p_path+'out','final_q')
        # if pathlib.Path.exists(toutpath):
        #     os.rmdir(toutpath)
        out_csv = out.coalesce(1).write.csv(toutpath, mode='overwrite', header=True)
        print('Final Output Persisted Successfully')

        return json.dumps(True), 202

if __name__ == '__main__':
    app.run(port = 6000, debug=True)