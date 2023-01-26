import json
import boto3
import random
import time
import os

from botocore.config import Config
from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext
from aws_lambda_powertools.utilities.data_classes import APIGatewayProxyEvent
from aws_lambda_powertools import Tracer
from aws_lambda_powertools import Metrics
from aws_lambda_powertools.metrics import MetricUnit


TIMESTREAM_DATABASE_NAME = os.environ['TIMESTREAM_DATABASE_NAME']
TIMESTREAM_TABLE_NAME = os.environ['TIMESTREAM_TABLE_NAME']

tracer = Tracer()
logger = Logger()


def current_milli_time():
    return round(time.time() * 1000)


# @metrics.log_metrics
@tracer.capture_lambda_handler
@logger.inject_lambda_context(log_event=False)
def lambda_handler(event, context):
    # TODO implement

    current_time = str(current_milli_time())

    deviceInfo = event.get('deviceInfo')
    deviceEvent = event.get('deviceEvent')
    placementInfo = event.get('placementInfo')

    logger.info(deviceInfo)
    logger.info(placementInfo)
    logger.info(deviceEvent)

    session = boto3.Session()
    timestream_client = session.client('timestream-write', region_name='us-east-1',
                                       config=Config(read_timeout=20,    # リクエストタイムアウト(秒)
                                                     max_pool_connections=5000,        # 最大接続数
                                                     retries={'max_attempts': 10}))    # 最大試行回数

    dimensions = [
        {'Name': 'deviceId', 'Value': deviceInfo['deviceId']},
        {'Name': 'placementName', 'Value': placementInfo['attributes']['placementName']},
    ]

    clickType = {
        'Dimensions': dimensions,
        'MeasureName': 'clickType',
        'MeasureValue': deviceEvent['buttonClicked']['clickType'],
        'MeasureValueType': 'VARCHAR',
        'Time': current_time
    }

    clickCount = {
        'Dimensions': dimensions,
        'MeasureName': 'clickCount',
        'MeasureValue': str(1),
        'MeasureValueType': 'BIGINT',
        'Time': current_time
    }

    records = [clickType, clickCount]
    # records = [clickCount]

    result = timestream_client.write_records(
        DatabaseName=TIMESTREAM_DATABASE_NAME,
        TableName=TIMESTREAM_TABLE_NAME,
        Records=records,
        CommonAttributes={})

    return {
        'statusCode': 200,
        'body': json.dumps(result)
    }
