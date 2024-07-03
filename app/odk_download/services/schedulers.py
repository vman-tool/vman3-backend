from loguru import logger

from app.odk_download.services.data_download import fetch_odk_data_with_async
from app.odk_download.services.data_tracker import get_incomplete_chunks


async def scheduled_failed_chucks_retry():
    logger.info("Scheduled retry of failed chunks initiated")
    await retry_failed_chunks()



# Function to retry failed chunks
async def retry_failed_chunks():
    failed_chunks = await get_incomplete_chunks()
    for chunk in failed_chunks:
        start_date = chunk['start_date']
        end_date = chunk['end_date']
        skip = chunk['skip']
        top = chunk['top']
        print(f"Retrying chunk: {start_date} - {end_date} - {skip} - {top}")
        await fetch_odk_data_with_async(
            start_date=start_date,
            end_date=end_date,
            skip=skip,
            top=top,
            resend=True
        )
        