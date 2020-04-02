from flask import Flask, request, abort, jsonify
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import os
from itertools import combinations
from pyspark.sql.functions import monotonically_increasing_id

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
        # print(query)
        # print(table)
        #load the table in the db
        tpath = os.path.join(p_path+'db', dbname, table[0])
        df = sqlContext.read.load(tpath+'.csv', format='csv', inferSchema='true', header='true')
        print('Table 1 loaded....')

        df.registerTempTable(table[0])
        left_len = len(df.columns)
        
        right_len = 0
        if len(table) == 4:
            print('Join Condition Identified')
            df.registerTempTable(table[1])
            #load the table in the db
            tpath = os.path.join(p_path+'db', dbname, table[2])
            df_j = sqlContext.read.load(tpath+'.csv', format='csv', inferSchema='true', header='true')
            print('Table 2 loaded...')
            df_j.registerTempTable(table[2])
            df_j.registerTempTable(table[3])
            
        
        #run SQL query here
        df = sqlContext.sql(query)
        print('Query Execution Completed')
        # df.show(10)
        #save in the partial output path
        # toutpath = os.path.join('tmp','f.csv')
        print('Removing duplicates from the query output')
        try:
            # df.printSchema()
            df_n = None
            #remove duplicate columns
            u_col = list(dict.fromkeys(df.columns))
            for col in u_col:
                print('Selecting...'+col)
                try:
                    if df_n == None:
                        df_n = df.select(col)
                    else:
                        t_m = df_n
                        t_m = t_m.withColumn('__id__', monotonically_increasing_id())
                        s_m = df.select(col).withColumn('__id__', monotonically_increasing_id())
                        df_n = t_m.join(s_m,'__id__','inner').drop('__id__')
                except:
                    if df_n == None:
                        df_n = df.select(table[1]+'.'+col)
                    else:
                        t_m = df_n
                        t_m = t_m.withColumn('__id__', monotonically_increasing_id())
                        s_m = df.select(table[1]+'.'+col).withColumn('__id__', monotonically_increasing_id())
                        df_n = t_m.join(s_m,'__id__','inner').drop('__id__')
            print('Removing Duplicates Done')
            # df_n = df_n.coalesce(1)
            # df_n.show(10)
            # df_n.printSchema()

            df_n.toPandas().to_csv(p_path+'tmp\db'+dbname+'.csv', index=False)
            print('Task Query Output Saved')
            # df.coalesce(1).write.csv(p_path+'tmp\db'+dbname+'.csv', mode='overwrite', header=True)
        except ValueError:
            print('Task Manager Interrupted')
            raise ValueError
        except Exception as e:
            print('Task Manager Interrupted')
            raise e
        
        data = {'lt': left_len}
        print(data)
        return jsonify(data), 202

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