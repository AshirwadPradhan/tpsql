import sys
import parser
import yaml
from aiohttp import ClientSession
import asyncio

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

    async with ClientSession() as session:
        for server in servers_list:
            # pass parsed_query[0] to task managers with the session
            
            pass


    #check if all the partial outputs returned True:
    # for all the True values get the values from the temp files,

    #send to aggregator

    # check if agg returned True and exit


