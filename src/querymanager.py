import sys
import yaml
from aiohttp import ClientSession
import asyncio
import taskmanager
import qparser
import os

async def process_query(raw_query:str) -> bool:
    ''' Manages execution of the query'''

    parsed_query = qparser.qparse(raw_query)
    print('Query Parsed')

    partial_op_checks = list()

    p_path = os.path.join('src','')
    try:
        with open(p_path+'server-conf.yaml', 'r') as file:
            servers_list = yaml.load(file, Loader=yaml.FullLoader)
    except FileNotFoundError:
        print('Server Config file missing...')
        raise FileNotFoundError
    print('Loaded Server Config')
    
    try:
        with open(p_path+'db-conf.yaml', 'r') as file:
            db_list = yaml.load(file, Loader=yaml.FullLoader)
    except FileNotFoundError:
        print('DB Config file missing...')
        raise FileNotFoundError
    print('Loaded DB config')

    print('Executing Task Managers...')
    async with ClientSession() as session:
        tasks = []
        for i in range(0,len(servers_list)):
            # pass parsed_query[0] to task managers with the session
            print('Starting Task Manager '+str(i))
            tasks.append(taskmanager.run_task(parsed_query[0], db_list[i], parsed_query[2], servers_list[i], session))
        
        print('All Task Managers Started')
        res = await asyncio.gather(*tasks, return_exceptions=True)
        print('All Task Managers execution completed')


    #check if all the partial outputs returned True:
    # for all the True values get the values from the temp files,

    left_len = None
    for i in range(0,len(res)):
        try:
            left_len = res[i].get('lt')
            print('Successfully executed: Task '+str(i) )
        except ValueError:
            #log the error
            print(' Error in executing Task '+str(i))
            print('Aggregator will result in partial output')
    
    #send to aggregator
    agg_url = 'http://localhost:6000/runagg'
    async with ClientSession() as session:
        json_data = {'query': parsed_query[1], 'table': parsed_query[2], 'lt': left_len}
        print('Starting Aggregation of Partial Outputs')
        async with session.post(agg_url, json=json_data) as response:
            try:
                r = await response.read()
                if response.status == 202:
                    print('Aggregation Completed')
                    return True
                else:
                    print('Error occured during Aggregation')
                    return False
            except Exception as e:
                print('Error occured during Aggregation \n'+ str(e))
    # check if agg returned True and exit
    return False

def proc_main(inp: str):
   return asyncio.run(process_query(inp), debug= True)