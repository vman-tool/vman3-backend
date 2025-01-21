from typing import List

from app.odk.services.data_download import insert_many_data_to_arangodb


async def insert_all_csv_data(data: List[dict]):

    try:
       return await insert_many_data_to_arangodb(data)

    except Exception as e:
        raise e
