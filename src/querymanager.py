import sys
import parser
import yaml
from aiohttp import ClientSession
import asyncio
import taskmanager

async def process_query(raw_query:str):
    ''' Manages execution of the query'''

    parsed_query = parser.parse(raw_query)

    partial_op_checks = list()

    try:
        with open('server-conf.yaml', 'r') as file:
            servers_list = yaml.load(file, Loader=yaml.FullLoader)
    except FileNotFoundError:
        print('Server Config file missing...')
        raise FileNotFoundError
    
    try:
        with open('db-conf.yaml', 'r') as file:
            db_list = yaml.load(file, Loader=yaml.FullLoader)
    except FileNotFoundError:
        print('DB Config file missing...')
        raise FileNotFoundError

    async with ClientSession() as session:
        tasks = []
        for i in range(0,len(servers_list)):
            # pass parsed_query[0] to task managers with the session
            tasks.append(taskmanager.run_task(parsed_query[0], db_list[i], parsed_query[2], servers_list[i], session))
        
        res = await asyncio.gather(*tasks, return_exceptions=True)


    #check if all the partial outputs returned True:
    # for all the True values get the values from the temp files,
    for i in range(0,len(res)):
        if res[i] == True:
            print('Task '+i+' is successful')
        else:
            print(' Error in executing Task '+i)
            print('Aggregator will result in partial output')
    
    #send to aggregator

    # check if agg returned True and exit


