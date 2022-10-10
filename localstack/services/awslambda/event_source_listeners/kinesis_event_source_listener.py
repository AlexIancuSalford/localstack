import base64
import json
import logging
from copy import deepcopy
from typing import Dict, List, Optional

from localstack.aws.accounts import get_aws_account_id
from localstack.services.awslambda.event_source_listeners.stream_event_source_listener import (
    StreamEventSourceListener,
)
from localstack.services.awslambda.lambda_api import get_event_sources
from localstack.services.awslambda.lambda_utils import (
    filter_stream_records,
    has_data_filter_criteria,
)
from localstack.utils.aws import aws_stack
from localstack.utils.common import first_char_to_lower, to_str
from localstack.utils.threads import FuncThread

LOG = logging.getLogger(__name__)


class KinesisEventSourceListener(StreamEventSourceListener):
    _FAILURE_PAYLOAD_DETAILS_FIELD_NAME = "KinesisBatchInfo"
    _COORDINATOR_THREAD: Optional[
        FuncThread
    ] = None  # Thread for monitoring state of event source mappings
    _STREAM_LISTENER_THREADS: Dict[
        str, FuncThread
    ] = {}  # Threads for listening to stream shards and forwarding data to mapped Lambdas

    @staticmethod
    def source_type() -> Optional[str]:
        return "kinesis"

    def _get_matching_event_sources(self) -> List[Dict]:
        event_sources = get_event_sources(source_arn=r".*:kinesis:.*")
        return [source for source in event_sources if source["State"] == "Enabled"]

    def _get_stream_client(self, region_name):
        return aws_stack.connect_to_service("kinesis", region_name=region_name)

    def _get_stream_description(self, stream_client, stream_arn):
        stream_name = stream_arn.split("/")[-1]
        return stream_client.describe_stream(StreamName=stream_name)["StreamDescription"]

    def _get_shard_iterator(self, stream_client, stream_arn, shard_id, iterator_type):
        stream_name = stream_arn.split("/")[-1]
        return stream_client.get_shard_iterator(
            StreamName=stream_name, ShardId=shard_id, ShardIteratorType=iterator_type
        )["ShardIterator"]

    def _filter_records(
        self, records: List[Dict], event_filter_criterias: List[Dict]
    ) -> List[Dict]:
        """
        https://docs.aws.amazon.com/lambda/latest/dg/invocation-eventfiltering.html
        - Parse data as json if any data filter pattern present.
        - Drop record if unable to parse.
        - When filtering, the key has to be "data"
        """
        if len(event_filter_criterias) == 0:
            return records

        if not has_data_filter_criteria(event_filter_criterias):
            # Lambda filters (on the other metadata properties only) based on your filter criteria.
            return filter_stream_records(records, event_filter_criterias)

        parsed_records = []
        for record in records:
            raw_data = record["Data"]
            try:
                # filters expect dict
                parsed_data = json.loads(raw_data)

                # remap "data" key for filtering
                parsed_record = deepcopy(record)
                del parsed_record["Data"]
                parsed_record["data"] = parsed_data

                parsed_records.append(parsed_record)
            except json.JSONDecodeError:
                LOG.warning(
                    f"Unable to convert record '{raw_data}' to json... Record will be dropped."
                )

        filtered_records = filter_stream_records(parsed_records, event_filter_criterias)

        # convert data back to bytes and remap key
        for filtered_record in filtered_records:
            parsed_data = filtered_record.pop("data")
            encoded_data = json.dumps(parsed_data).encode()
            filtered_record["Data"] = encoded_data

        return filtered_records

    def _create_lambda_event_payload(self, stream_arn, records, shard_id=None):
        record_payloads = []
        for record in records:
            record_payload = {}
            for key, val in record.items():
                record_payload[first_char_to_lower(key)] = val
            # boto3 automatically decodes records in get_records(), so we must re-encode
            record_payload["data"] = to_str(base64.b64encode(record_payload["data"]))
            # convert datetime obj to timestamp
            record_payload["approximateArrivalTimestamp"] = (
                record_payload["approximateArrivalTimestamp"].timestamp() * 1000
            )
            record_payloads.append(
                {
                    "eventID": "{0}:{1}".format(shard_id, record_payload["sequenceNumber"]),
                    "eventSourceARN": stream_arn,
                    "eventSource": "aws:kinesis",
                    "eventVersion": "1.0",
                    "eventName": "aws:kinesis:record",
                    "invokeIdentityArn": "arn:aws:iam::{0}:role/lambda-role".format(
                        get_aws_account_id()
                    ),
                    "awsRegion": aws_stack.get_region(),
                    "kinesis": record_payload,
                }
            )
        return {"Records": record_payloads}

    def _get_starting_and_ending_sequence_numbers(self, first_record, last_record):
        return first_record["SequenceNumber"], last_record["SequenceNumber"]

    def _get_first_and_last_arrival_time(self, first_record, last_record):
        return (
            first_record["ApproximateArrivalTimestamp"].isoformat() + "Z",
            last_record["ApproximateArrivalTimestamp"].isoformat() + "Z",
        )
