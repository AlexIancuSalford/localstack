"""
Test file for HTTP Task Service in Step Functions.
This tests the HTTP task functionality in LocalStack.
"""
import json
import pytest
from localstack.testing.pytest import markers
from localstack.utils.strings import short_uid
from tests.aws.services.stepfunctions.templates.services.services_templates import (
    ServicesTemplates as ST,
)
from localstack.testing.pytest.stepfunctions.utils import (
    create_and_record_execution,
)


@markers.snapshot.skip_snapshot_verify(
    paths=[
        # HTTP responses can vary slightly between calls
        "$..Headers.Date",
        "$..Headers.x-amzn-RequestId",
        "$..Headers.X-Amzn-Trace-Id",
    ]
)
class TestTaskHttp:
    """Test suite for HTTP task functionality in Step Functions."""
    
    @markers.aws.validated
    def test_http_get_request(
        self,
        aws_client,
        create_state_machine,
        create_state_machine_iam_role,
        sfn_snapshot,
    ):
        """Test basic HTTP GET request through Step Functions."""
        
        # Create a simple state machine with an HTTP task
        definition = {
            "Comment": "Test HTTP GET request",
            "StartAt": "HttpGetTask",
            "States": {
                "HttpGetTask": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::http:invoke",
                    "Parameters": {
                        "Method": "GET",
                        "Url": "https://httpbin.org/get",
                        "Headers": {
                            "Accept": "application/json"
                        }
                    },
                    "End": True
                }
            }
        }
        
        exec_input = json.dumps({})
        create_and_record_execution(
            aws_client,
            create_state_machine_iam_role,
            create_state_machine,
            sfn_snapshot,
            json.dumps(definition),
            exec_input,
        )
    
    @markers.aws.validated
    def test_http_post_with_json_body(
        self,
        aws_client,
        create_state_machine,
        create_state_machine_iam_role,
        sfn_snapshot,
    ):
        """Test HTTP POST request with JSON body."""
        
        definition = {
            "Comment": "Test HTTP POST request with JSON body",
            "StartAt": "HttpPostTask",
            "States": {
                "HttpPostTask": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::http:invoke",
                    "Parameters": {
                        "Method": "POST",
                        "Url": "https://httpbin.org/post",
                        "Headers": {
                            "Content-Type": "application/json"
                        },
                        "RequestBody": {
                            "name": "Test User",
                            "email": "test@example.com"
                        }
                    },
                    "End": True
                }
            }
        }
        
        exec_input = json.dumps({})
        create_and_record_execution(
            aws_client,
            create_state_machine_iam_role,
            create_state_machine,
            sfn_snapshot,
            json.dumps(definition),
            exec_input,
        )
    
    @markers.aws.validated
    def test_http_with_query_parameters(
        self,
        aws_client,
        create_state_machine,
        create_state_machine_iam_role,
        sfn_snapshot,
    ):
        """Test HTTP request with query parameters."""
        
        definition = {
            "Comment": "Test HTTP request with query parameters",
            "StartAt": "HttpQueryTask",
            "States": {
                "HttpQueryTask": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::http:invoke",
                    "Parameters": {
                        "Method": "GET",
                        "Url": "https://httpbin.org/get",
                        "QueryParameters": {
                            "param1": "value1",
                            "param2": "value2"
                        }
                    },
                    "End": True
                }
            }
        }
        
        exec_input = json.dumps({})
        create_and_record_execution(
            aws_client,
            create_state_machine_iam_role,
            create_state_machine,
            sfn_snapshot,
            json.dumps(definition),
            exec_input,
        )
    
    @markers.aws.validated
    def test_http_with_authentication(
        self,
        aws_client,
        create_state_machine,
        create_state_machine_iam_role,
        sfn_snapshot,
    ):
        """Test HTTP request with Bearer token authentication."""
        
        definition = {
            "Comment": "Test HTTP request with authentication",
            "StartAt": "HttpAuthTask",
            "States": {
                "HttpAuthTask": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::http:invoke",
                    "Parameters": {
                        "Method": "GET",
                        "Url": "https://httpbin.org/bearer",
                        "Authentication": {
                            "Type": "Bearer",
                            "Token": "test-token-12345"
                        }
                    },
                    "End": True
                }
            }
        }
        
        exec_input = json.dumps({})
        create_and_record_execution(
            aws_client,
            create_state_machine_iam_role,
            create_state_machine,
            sfn_snapshot,
            json.dumps(definition),
            exec_input,
        )
    
    @markers.aws.validated
    def test_http_error_handling(
        self,
        aws_client,
        create_state_machine,
        create_state_machine_iam_role,
        sfn_snapshot,
    ):
        """Test HTTP task error handling for 404 response."""
        
        definition = {
            "Comment": "Test HTTP error handling",
            "StartAt": "HttpErrorTask",
            "States": {
                "HttpErrorTask": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::http:invoke",
                    "Parameters": {
                        "Method": "GET",
                        "Url": "https://httpbin.org/status/404"
                    },
                    "Catch": [{
                        "ErrorEquals": ["States.Http.404"],
                        "Next": "HandleError"
                    }],
                    "End": True
                },
                "HandleError": {
                    "Type": "Pass",
                    "Result": "Error handled",
                    "End": True
                }
            }
        }
        
        exec_input = json.dumps({})
        create_and_record_execution(
            aws_client,
            create_state_machine_iam_role,
            create_state_machine,
            sfn_snapshot,
            json.dumps(definition),
            exec_input,
        )
    
    @markers.aws.validated
    def test_http_with_dynamic_input(
        self,
        aws_client,
        create_state_machine,
        create_state_machine_iam_role,
        sfn_snapshot,
    ):
        """Test HTTP task with dynamic input from state machine input."""
        
        definition = {
            "Comment": "Test HTTP with dynamic input",
            "StartAt": "HttpDynamicTask",
            "States": {
                "HttpDynamicTask": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::http:invoke",
                    "Parameters": {
                        "Method.$": "$.method",
                        "Url.$": "$.url",
                        "RequestBody.$": "$.body"
                    },
                    "End": True
                }
            }
        }
        
        # Start execution with dynamic input
        input_data = {
            "method": "POST",
            "url": "https://httpbin.org/post",
            "body": {
                "message": "Dynamic input test"
            }
        }
        
        exec_input = json.dumps(input_data)
        create_and_record_execution(
            aws_client,
            create_state_machine_iam_role,
            create_state_machine,
            sfn_snapshot,
            json.dumps(definition),
            exec_input,
        )

    @markers.aws.validated  
    def test_http_with_timeout(
        self,
        aws_client,
        create_state_machine,
        create_state_machine_iam_role,
        sfn_snapshot,
    ):
        """Test HTTP task with connection timeout configuration."""
        
        definition = {
            "Comment": "Test HTTP with timeout",
            "StartAt": "HttpTimeoutTask",
            "States": {
                "HttpTimeoutTask": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::http:invoke",
                    "Parameters": {
                        "Method": "GET",
                        "Url": "https://httpbin.org/delay/1",
                        "ConnectionConfig": {
                            "ConnectionTimeout": 5,
                            "ResponseTimeout": 5
                        }
                    },
                    "TimeoutSeconds": 10,
                    "End": True
                }
            }
        }
        
        exec_input = json.dumps({})
        create_and_record_execution(
            aws_client,
            create_state_machine_iam_role,
            create_state_machine,
            sfn_snapshot,
            json.dumps(definition),
            exec_input,
        )

    @markers.aws.validated
    def test_http_transform_disabled(
        self,
        aws_client,
        create_state_machine,
        create_state_machine_iam_role,
        sfn_snapshot,
    ):
        """Test HTTP task with Transform disabled (no auto JSON parsing)."""
        
        definition = {
            "Comment": "Test HTTP with Transform disabled",
            "StartAt": "HttpNoTransformTask",
            "States": {
                "HttpNoTransformTask": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::http:invoke",
                    "Parameters": {
                        "Method": "GET",
                        "Url": "https://httpbin.org/json",
                        "Transform": False
                    },
                    "End": True
                }
            }
        }
        
        exec_input = json.dumps({})
        create_and_record_execution(
            aws_client,
            create_state_machine_iam_role,
            create_state_machine,
            sfn_snapshot,
            json.dumps(definition),
            exec_input,
        )
