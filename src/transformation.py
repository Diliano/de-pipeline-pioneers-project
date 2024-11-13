import logging


logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    logger.info("Transformation lambda triggered")

    # Implement code


    return {"statusCode": 200,
            "body": "Transformation complete"
        }