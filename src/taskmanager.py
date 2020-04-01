import asyncio
from aiohttp import ClientSession

async def run_task(query: str, dbname: str, table: str, url: str, session: ClientSession):

    json_data = {'query': query, 'dbname': dbname, 'table': table}

    async with session.post(url, data=json_data) as response:
        response = await response.read()
        if response.status == 202:
            return True
        else:
            return False