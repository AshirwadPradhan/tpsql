import asyncio
from aiohttp import ClientSession

async def run_task(query: str, dbname: str, table: list, url: str, session: ClientSession):

    json_data = {'query': query, 'dbname': dbname, 'table': table}

    async with session.post(url, json=json_data) as response:
        r = await response.read()
        if response.status == 202:
            return True
        else:
            return False