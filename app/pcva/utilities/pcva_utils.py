from arango.database import StandardDatabase

from app.pcva.models.pcva_models import PCVAConfigurations
from app.pcva.requests.configurations_request_classes import PCVAConfigurationsRequest
async def fetch_pcva_settings(db: StandardDatabase = None):
    configs = await PCVAConfigurations.get_many(db=db)
    if len(configs) == 0 or not configs:
        raise ValueError("No PCVA configurations found")
    return PCVAConfigurationsRequest(**configs[0])