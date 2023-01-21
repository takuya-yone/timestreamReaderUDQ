# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved. 2021
# SPDX-License-Identifier: Apache-2.0

import logging
import os
import sys
from datetime import datetime

import boto3

from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext
from aws_lambda_powertools.utilities.data_classes import APIGatewayProxyEvent
from aws_lambda_powertools import Tracer
from aws_lambda_powertools import Metrics
from aws_lambda_powertools.metrics import MetricUnit

from udq_utils.udq import SingleEntityReader, MultiEntityReader, IoTTwinMakerDataRow, IoTTwinMakerUdqResponse
from udq_utils.udq_models import IoTTwinMakerUDQEntityRequest, IoTTwinMakerUDQComponentTypeRequest, OrderBy, IoTTwinMakerReference, \
    EntityComponentPropertyRef, ExternalIdPropertyRef

from udq_utils.sql_detector import SQLDetector

tracer = Tracer()
logger = Logger()


class TimestreamReader(SingleEntityReader, MultiEntityReader):
    def __init__(self, query_client, database_name, table_name):
        self.query_client = query_client
        self.database_name = database_name
        self.table_name = table_name
        self.sqlDetector = SQLDetector()

    # overrides SingleEntityReader.entity_query abstractmethod
    def entity_query(self, request: IoTTwinMakerUDQEntityRequest):
        logger.info("TimestreamReader entity_query")
        logger.info(request.udq_context['properties'])

        selected_properties = request.selected_properties
        property_filter = request.property_filters[0] if request.property_filters else None
        filter_clause = f"AND measure_value::varchar {property_filter['operator']} '{property_filter['value']['stringValue']}'" if property_filter else ""

        telemetry_asset_type = request.udq_context['properties']['placementName']['value']['stringValue']
        telemetry_asset_id = request.udq_context['properties']['deviceId']['value']['stringValue']

        # e.g. "p0", "p1", ...
        sample_sel_properties = [
            f"p{x}" for x in range(
                0, len(selected_properties))]
        sample_measure_name_clause = " OR ".join(
            [f"measure_name = '{x}'" for x in sample_sel_properties])
        measure_name_clause = " OR ".join(
            [f"measure_name = '{x}'" for x in selected_properties])
        if property_filter:
            sample_query = f"""SELECT deviceId, measure_name, time, measure_value::double, measure_value::varchar FROM "CookieFactoryTelemetry"."Telemetry"  WHERE time > from_iso8601_timestamp('2021-10-18T21:42:58') AND time <= from_iso8601_timestamp('2021-10-18T21:43:35') AND placementName = 'test' AND deviceId = 'test' AND ({sample_measure_name_clause}) AND measure_value::varchar {property_filter['operator']} 'abc'  ORDER BY time ASC"""
        else:
            sample_query = f"""SELECT deviceId, measure_name, time, measure_value::double, measure_value::varchar FROM "CookieFactoryTelemetry"."Telemetry"  WHERE time > from_iso8601_timestamp('2021-10-18T21:42:58') AND time <= from_iso8601_timestamp('2021-10-18T21:43:35') AND placementName = 'test' AND deviceId = 'test' AND ({sample_measure_name_clause})   ORDER BY time ASC"""

        query_string = f"SELECT deviceId, measure_name, time, measure_value::double, measure_value::varchar" \
                       f""" FROM "{self.database_name}"."{self.table_name}" """ \
                       f""" WHERE time > from_iso8601_timestamp('{request.start_time}')""" \
                       f""" AND time <= from_iso8601_timestamp('{request.end_time}')""" \
                       f""" AND placementName = '{telemetry_asset_type}'""" \
                       f""" AND deviceId = '{telemetry_asset_id}'""" \
                       f""" AND ({measure_name_clause})""" \
                       f""" {filter_clause} """ \
                       f""" ORDER BY time {'ASC' if request.order_by == OrderBy.ASCENDING else 'DESC'}"""

        self.sqlDetector.detectInjection(sample_query, query_string)

        page = self._run_timestream_query(
            query_string, request.next_token, request.max_rows)
        return self._convert_timestream_query_page_to_udq_response(
            page, request.entity_id, request.component_name, telemetry_asset_type)

    # overrides MultiEntityReader.component_type_query abstractmethod
    def component_type_query(
            self,
            request: IoTTwinMakerUDQComponentTypeRequest):
        logger.info("TimestreamReader component_type_query")

        selected_properties = request.selected_properties
        property_filter = request.property_filters[0] if request.property_filters else None
        filter_clause = f"AND measure_value::varchar {property_filter['operator']} '{property_filter['value']['stringValue']}'" if property_filter else ""
        telemetry_asset_type = request.udq_context['properties']['placementName']['value']['stringValue']

        # e.g. "p0", "p1", ...
        sample_sel_properties = [
            f"p{x}" for x in range(
                0, len(selected_properties))]
        sample_measure_name_clause = " OR ".join(
            [f"measure_name = '{x}'" for x in sample_sel_properties])
        measure_name_clause = " OR ".join(
            [f"measure_name = '{x}'" for x in selected_properties])
        if property_filter:
            sample_query = f"""SELECT deviceId, measure_name, time, measure_value::double, measure_value::varchar FROM "CookieFactoryTelemetry"."Telemetry"  WHERE time > from_iso8601_timestamp('2021-10-18T21:42:58') AND time <= from_iso8601_timestamp('2021-10-18T21:43:35') AND placementName = 'test' AND ({sample_measure_name_clause}) AND measure_value::varchar {property_filter['operator']} 'abc'  ORDER BY time ASC"""
        else:
            sample_query = f"""SELECT deviceId, measure_name, time, measure_value::double, measure_value::varchar FROM "CookieFactoryTelemetry"."Telemetry"  WHERE time > from_iso8601_timestamp('2021-10-18T21:42:58') AND time <= from_iso8601_timestamp('2021-10-18T21:43:35') AND placementName = 'test' AND ({sample_measure_name_clause})   ORDER BY time ASC"""

        query_string = f"SELECT deviceId, measure_name, time, measure_value::double, measure_value::varchar" \
                       f""" FROM "{self.database_name}"."{self.table_name}" """ \
                       f""" WHERE time > from_iso8601_timestamp('{request.start_time}')""" \
                       f""" AND time <= from_iso8601_timestamp('{request.end_time}')""" \
                       f""" AND placementName = '{telemetry_asset_type}'""" \
                       f""" AND ({measure_name_clause})""" \
                       f""" {filter_clause} """ \
                       f""" ORDER BY time {'ASC' if request.order_by == OrderBy.ASCENDING else 'DESC'}"""

        self.sqlDetector.detectInjection(sample_query, query_string)

        page = self._run_timestream_query(
            query_string, request.next_token, request.max_rows)
        return self._convert_timestream_query_page_to_udq_response(
            page, request.entity_id, request.component_name, telemetry_asset_type)

    def _run_timestream_query(self, query_string, next_token, max_rows):
        logger.info(
            "Query string is %s , next token is %s",
            query_string,
            next_token)
        try:
            # Timestream SDK returns error if None is passed for NextToken and
            # MaxRows
            if next_token and max_rows:
                page = self.query_client.query(
                    QueryString=query_string,
                    NextToken=next_token,
                    MaxRows=max_rows)
            elif next_token:
                page = self.query_client.query(
                    QueryString=query_string, NextToken=next_token)
            elif max_rows:
                page = self.query_client.query(
                    QueryString=query_string, MaxRows=max_rows)
                while 'NextToken' in page and len(page['Rows']) == 0:
                    page = self.query_client.query(
                        QueryString=query_string,
                        NextToken=page['NextToken'],
                        MaxRows=max_rows)
            else:
                page = self.query_client.query(QueryString=query_string)

            return page

        except Exception as err:
            logger.error("Exception while running query: %s", err)
            raise err

    @staticmethod
    def _convert_timestream_query_page_to_udq_response(
            query_page, entity_id, component_name, telemetry_asset_type):
        logger.info("Query result is %s", query_page)
        result_rows = []
        schema = query_page['ColumnInfo']
        for row in query_page['Rows']:
            result_rows.append(
                TimestreamDataRow(
                    row,
                    schema,
                    entity_id,
                    component_name,
                    telemetry_asset_type))
        return IoTTwinMakerUdqResponse(
            result_rows, query_page.get('NextToken'))


class TimestreamDataRow(IoTTwinMakerDataRow):

    def __init__(
            self,
            timestream_row,
            timestream_column_schema,
            entity_id=None,
            component_name=None,
            _telemetry_asset_type=None):
        self._timestream_row = timestream_row
        self._timestream_column_schema = timestream_column_schema
        self._row_as_dict = self._parse_row(
            timestream_column_schema, timestream_row)
        self._entity_id = entity_id
        self._component_name = component_name
        self._telemetry_asset_type = _telemetry_asset_type

    # overrides IoTTwinMakerDataRow.get_iottwinmaker_reference abstractmethod
    def get_iottwinmaker_reference(self):
        property_name = self._row_as_dict['measure_name']
        if self._entity_id and self._component_name:
            return IoTTwinMakerReference(
                ecp=EntityComponentPropertyRef(
                    self._entity_id,
                    self._component_name,
                    property_name))
        else:
            external_id_property = {
                # special case Alarm and map the externalId to alarm_key
                'alarm_key' if self._telemetry_asset_type == 'Alarm' else 'deviceId': self._row_as_dict['deviceId'],
            }
            return IoTTwinMakerReference(
                eip=ExternalIdPropertyRef(
                    external_id_property, property_name))

    # overrides IoTTwinMakerDataRow.get_iso8601_timestamp abstractmethod
    def get_iso8601_timestamp(self):
        return self._row_as_dict['time'].replace(' ', 'T') + 'Z'

    # overrides IoTTwinMakerDataRow.get_value abstractmethod
    def get_value(self):
        if 'measure_value::varchar' in self._row_as_dict and self._row_as_dict[
                'measure_value::varchar'] is not None:
            return self._row_as_dict['measure_value::varchar']
        elif 'measure_value::double' in self._row_as_dict and self._row_as_dict['measure_value::double'] is not None:
            return float(self._row_as_dict['measure_value::double'])
        else:
            raise ValueError(
                f"Unhandled type in timestream row: {self._row_as_dict}")

    def _parse_row(self, column_schema, timestream_row):
        data = timestream_row['Data']
        result = {}
        for i in range(len(data)):
            info = column_schema[i]
            datum = data[i]
            key, val = self._parse_datum(info, datum)
            result[key] = val
        return result

    @staticmethod
    def _parse_datum(info, datum):
        if datum.get('NullValue', False):
            return info['Name'], None
        column_type = info['Type']
        if 'ScalarType' in column_type:
            return info['Name'], datum['ScalarValue']
        else:
            raise Exception(f"Unsupported columnType[{column_type}]")


SESSION = boto3.Session()
QUERY_CLIENT = SESSION.client('timestream-query')

# retrieve database name and table name from Lambda environment variables
# check if running on Lambda
if os.environ.get("AWS_EXECUTION_ENV") is not None:
    DATABASE_NAME = os.environ['TIMESTREAM_DATABASE_NAME']
    TABLE_NAME = os.environ['TIMESTREAM_TABLE_NAME']
else:
    # logger.addHandler(logging.StreamHandler(sys.stdout))
    DATABASE_NAME = None
    TABLE_NAME = None

TIMESTREAM_UDQ_READER = TimestreamReader(
    QUERY_CLIENT, DATABASE_NAME, TABLE_NAME)


# Main Lambda invocation entry point, use the TimestreamReader to process events
# noinspection PyUnusedLocal
def lambda_handler(event, context):
    logger.info("Event:")
    logger.info(event)
    result = TIMESTREAM_UDQ_READER.process_query(event)
    logger.info("result:")
    logger.info(result)
    return result
