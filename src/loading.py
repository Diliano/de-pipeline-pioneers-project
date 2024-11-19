import logging
import json

# Logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    logger.info("Loading lambda: %s", json.dumps(event))

    # Placeholder for loading logic 

    return {"status": "Success"}