from src.loading.code.s3_utils import read_file_list, process_parquet_files
from src.loading.code.db_utils import connect_to_db, load_data_into_warehouse
import boto3
import logging


logger = logging.getLogger()
logger.setLevel(logging.INFO)

SECRET_NAME = "test_db_secret"
AWS_REGION = "eu-west-2"
S3_PROCESSED_BUCKET = "test-processed-bucket"
FILE_LIST_KEY = "test-processed/file_list.json"

s3_client = boto3.client("s3", region_name=AWS_REGION)


def lambda_handler(event, context):
    try:
        file_paths = read_file_list(
            s3_client, S3_PROCESSED_BUCKET, FILE_LIST_KEY
        )
        if not file_paths:
            logger.info("No files to process this time.")
            return {
                "status": "Success",
                "message": "No files to process this time.",
            }

        tables_data_frames = process_parquet_files(s3_client, file_paths)

        if not tables_data_frames:
            return {
                "status": "Failure",
                "message": (
                    "Failed to process any Parquet files. "
                    "Check logs for details."
                ),
            }

        if len(tables_data_frames) < len(file_paths):
            logger.warning(
                "Partial success: Some Parquet files could not be processed. "
                "Check logs for details."
            )
        else:
            logger.info("All Parquet files processed successfully.")

        with connect_to_db(SECRET_NAME, AWS_REGION) as conn:
            results = load_data_into_warehouse(conn, tables_data_frames)

        if not results["successfully_loaded"]:
            logger.error("Failure in loading data into the warehouse.")
            return {
                "status": "Failure",
                "message": (
                    "Failed to load any data into the warehouse. "
                    "Check logs for details."
                ),
                "results": results,
            }

        if results["failed_to_load"] or results["skipped_empty"]:
            logger.warning(
                "Partial success in loading data into the warehouse."
            )
            return {
                "status": "Partial",
                "message": (
                    "Partial success loading data into the warehouse. "
                    "Check logs for details."
                ),
                "results": results,
            }

        logger.info("All data loaded successfully into the warehouse.")
        return {
            "status": "Success",
            "message": "All data loaded successfully into the warehouse.",
            "results": results,
        }

    except Exception as e:
        logger.error(f"Lambda execution failed: {e}", exc_info=True)
        return {
            "status": "Failure",
            "message": "Lambda execution failed. Check logs for details.",
        }
