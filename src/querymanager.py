import sys
import yaml
from aiohttp import ClientSession
import asyncio
import taskmanager
import qparser
import os
import time

async def process_query(raw_query:str, sync_flag=False) -> bool:
    ''' Manages execution of the query'''
    
    start = time.time()
    parsed_query = qparser.qparse(raw_query)
    end = time.time()
    print('Query Parsed')
    print(f'Time taken for Parsing ---> {end-start} seconds')

    partial_op_checks = list()

    start = time.time()
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
    end = time.time()
    print(f'Time taken for Loading configuration --> {end-start} seconds')

    if sync_flag == False:
        start = time.time()
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
        end = time.time()
        print(f'Time taken for all Async tasks to finish --> {end-start} seconds')
    else:
        left_len = 0 
        start = time.time()
        from staskmanager import sync_run_task
        state = []
        try:
            print('Starting Sequential Execution')
            for i in range(0,len(servers_list)):
                (f'Task {i} started')
                res = sync_run_task(parsed_query[0], db_list[i], parsed_query[2], servers_list[i])
                state.append(res)
            total_success_task = 0
            for r in state:
                print(r)
                if r is not None:
                    left_len = r.get('lt', left_len)
                    total_success_task = total_success_task + 1
            print(f'Total {total_success_task} are successful...')
        except Exception as e:
            print(f'Total {total_success_task} are successful...')
            print('Some exception occurred during sync execution')
            print(e)
        end = time.time()
        print(f'Time taken for all the Sync tasks --> {end-start} seconds')


    #send to aggregator
    start = time.time()
    agg_url = 'http://localhost:6000/runagg'
    async with ClientSession() as session:
        json_data = {'query': parsed_query[1], 'table': parsed_query[2], 'lt': left_len}
        print('Starting Aggregation of Partial Outputs')
        async with session.post(agg_url, json=json_data) as response:
            try:
                r = await response.read()
                if response.status == 202:
                    print('Aggregation Completed')
                    end = time.time()
                    print(f'Time taken for successful aggregation --> {end-start} seconds')
                    return True
                else:
                    end = time.time()
                    print(f'Time taken for unsuccessful aggregation --> {end-start} seconds')
                    print('Error occured during Aggregation')
                    return False
            except Exception as e:
                end = time.time()
                print(f'Time taken for excepted aggregation --> {end-start} seconds')
                print('Error occured during Aggregation \n'+ str(e))
    # check if agg returned True and exit
    return False

def proc_main(inp: str):
   return asyncio.run(process_query(inp, False), debug= True)