// Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

import argparse
import logging
from pyspark.sql import SparkSession


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')


def process_data(input_uri, output_uri):
    """
    This function reads csv file from one S3 location and copies to another location.

    :param input_uri: input data location using S3 access point scheme
    :param output_uri: output data location using S3 access point scheme.
    """
    logger.info("Reading input data from S3 via Access Point")
    with SparkSession.builder \
            .appName('Cross-account S3 and EMR example using access points') \
            .getOrCreate() as spark:
        input_df = spark.read.option('header', 'true').csv(input_uri)
        logger.info("Writing output to S3 via Access Point")
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
