import requests

def sync_run_task(query: str, dbname: str, table: list, url: str):
    json_data = {'query': query, 'dbname': dbname, 'table': table}
    try:
        r = requests.post(url=url, json=json_data)
        if r.status_code == 202:
            return r.json()
        else:
            return None
    except Exception as e:
        raise e
