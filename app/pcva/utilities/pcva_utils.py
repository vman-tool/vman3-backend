from arango.database import StandardDatabase

from app.pcva.models.pcva_models import PCVAConfigurations
from app.pcva.requests.configurations_request_classes import PCVAConfigurationsRequest
from app.utilits.logger import app_logger

# Used until an administrator saves PCVA Configuration for the first time.
# These match the defaults the settings form itself shows, so the values in
# effect before anyone visits that page are the same ones they will see there.
#
# Handed out as a copy, never as this object: it is module-level and pydantic
# models are mutable, so a caller assigning to an attribute would rewrite the
# default for every request in the process. No caller does that today, which
# is precisely why it would be hard to find if one ever did.
_DEFAULT_PCVA_SETTINGS = PCVAConfigurationsRequest(
    useICD11=False,
    vaAssignmentLimit=2,
    concordanceLevel=2,
    showOtherCodersWork=True,
    enableMLIntegration=False,
)


async def fetch_pcva_settings(db: StandardDatabase = None):
    """PCVA configuration, falling back to defaults when none is stored.

    This used to raise ValueError("No PCVA configurations found"), which is the
    state of every freshly installed deployment: the collection is empty until
    somebody opens Settings > PCVA Configuration and presses Save. Five call
    sites depend on this - assignment, unassignment, coded VAs, concordance -
    so the whole PCVA module failed on first boot with an error that named a
    settings page rather than telling anyone to visit it.

    Returning defaults makes the module usable immediately, and saving the page
    still overrides them.
    """
    configs = await PCVAConfigurations.get_many(db=db)
    if not configs:
        app_logger.info(
            "No PCVA configuration stored yet; using defaults "
            f"(assignment limit {_DEFAULT_PCVA_SETTINGS.vaAssignmentLimit}, "
            f"concordance {_DEFAULT_PCVA_SETTINGS.concordanceLevel}). "
            "Save Settings > PCVA Configuration to set your own."
        )
        return _DEFAULT_PCVA_SETTINGS.model_copy()
    return PCVAConfigurationsRequest(**configs[0])
