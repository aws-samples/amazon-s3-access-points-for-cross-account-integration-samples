import argparse
import logging
from pyspark.sql import SparkSession


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')


def process_data(input_uri, output_uri):
    """
    Reads csv data stored in Amazon S3 and writes back to Amazon S3 via Access Points.

    :param input_uri: The S3 Access Points URI from where input data is read.
    :param output_uri: The S3 Access Points URI where the output is written.
    """
    logger.info("Reading input data from S3 via Access Points")
    with SparkSession.builder \
            .appName('S3 Access Points Example') \
            .getOrCreate() as spark:
        input_df = spark.read.option('header', 'true').csv(input_uri)
        logger.info("Writing output to S3 via Access Points")
        input_df.write.option('header', 'true') \
            .mode('overwrite') \
            .csv(output_uri)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input_uri', help="The S3 Access Points URI from where input data is read")
    parser.add_argument(
        '--output_uri', help="The S3 Access Points URI where output is saved.")
    args = parser.parse_args()
    process_data(args.input_uri, args.output_uri)
