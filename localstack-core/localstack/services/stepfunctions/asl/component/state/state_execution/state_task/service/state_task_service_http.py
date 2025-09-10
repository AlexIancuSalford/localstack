"""
HTTP Task Service implementation for AWS Step Functions in LocalStack.
This provides support for HTTP task states that can make HTTP/HTTPS requests to external endpoints.

This implementation inherits directly from StateTask instead of StateTaskService,
avoiding unnecessary AWS service-specific logic.
"""
import json
import logging
from typing import Any, Dict, Final, Optional
from urllib.parse import urlencode

import requests
from requests.auth import HTTPBasicAuth

# EventBridge integration imports
from localstack.services.stepfunctions.asl.eval.callback.callback import CallbackEndpoint
from localstack.services.stepfunctions.asl.utils.encoding import to_json_str
from localstack.utils.strings import long_uid
from localstack.utils.time import TIMESTAMP_FORMAT_TZ, timestamp
from localstack.http import Request
from localstack.services.events.models import events_stores
from localstack.services.events.utils import format_event
from localstack.utils.strings import long_uid
from localstack.services.events.provider import EventsProvider
from localstack.services.events.event_bus import EventBusService
from localstack.services.stepfunctions.asl.eval.callback.callback import (
    CallbackOutcomeSuccess,
    CallbackOutcomeFailure,
    CallbackOutcomeTimedOut
)

from localstack.services.stepfunctions.asl.component.common.error_name.custom_error_name import (
    CustomErrorName,
)
from localstack.aws.api.stepfunctions import (
    HistoryEventExecutionDataDetails,
    HistoryEventType,
    TaskCredentials,
    TaskFailedEventDetails,
    TaskScheduledEventDetails,
    TaskStartedEventDetails,
    TaskSucceededEventDetails,
    TaskTimedOutEventDetails,
)
from localstack.services.stepfunctions.asl.component.common.error_name.custom_error_name import (
    CustomErrorName,
)
from localstack.services.stepfunctions.asl.component.common.error_name.failure_event import (
    FailureEvent,
    FailureEventException,
)
from localstack.services.stepfunctions.asl.component.common.error_name.states_error_name import (
    StatesErrorName,
)
from localstack.services.stepfunctions.asl.component.common.error_name.states_error_name_type import (
    StatesErrorNameType,
)
from localstack.services.stepfunctions.asl.component.state.state_execution.state_task.credentials import (
    StateCredentials,
)
from localstack.services.stepfunctions.asl.component.state.state_execution.state_task.mock_eval_utils import (
    eval_mocked_response,
)
from localstack.services.stepfunctions.asl.component.state.state_execution.state_task.service.resource import (
    ResourceCondition,
    ResourceRuntimePart,
    ServiceResource,
)
from localstack.services.stepfunctions.asl.component.state.state_execution.state_task.state_task import (
    StateTask,
)
from localstack.services.stepfunctions.asl.component.state.state_props import StateProps
from localstack.services.stepfunctions.asl.eval.environment import Environment
from localstack.services.stepfunctions.asl.eval.event.event_detail import EventDetails
from localstack.services.stepfunctions.asl.utils.encoding import to_json_str
from localstack.services.stepfunctions.mocking.mock_config import MockedResponse
from localstack.services.stepfunctions.quotas import is_within_size_quota

LOG = logging.getLogger(__name__)

# Supported parameters for HTTP:invoke
_SUPPORTED_API_PARAM_BINDINGS: Final[dict[str, set[str]]] = {
    "invoke": {
        "Method",           # HTTP method: GET, POST, PUT, DELETE, PATCH, HEAD, OPTIONS
        "Url",             # The URL to make the request to
        "Headers",         # Optional: HTTP headers
        "RequestBody",     # Optional: Body for POST/PUT/PATCH requests
        "QueryParameters", # Optional: Query parameters to append to URL
        "Authentication",  # Optional: Authentication configuration
        "ConnectionConfig", # Optional: Connection configuration (timeouts, etc.)
        "Transform",       # Optional: Enable/disable automatic JSON transformation
        "EventBridge",     # Optional: EventBridge integration configuration
    }
}

# Supported integration patterns
_SUPPORTED_INTEGRATION_PATTERNS: Final[set[ResourceCondition]] = {
    ResourceCondition.WaitForTaskToken,  # HTTP with EventBridge wait task token
}

# Default timeout values (in seconds)
DEFAULT_CONNECTION_TIMEOUT = 60
DEFAULT_RESPONSE_TIMEOUT = 60


class StateTaskServiceHttp(StateTask):
    """
    HTTP Task Service implementation for Step Functions.

    This service allows Step Functions to make HTTP/HTTPS requests to external endpoints
    as part of the state machine execution.

    Inherits directly from StateTask to avoid AWS service-specific logic.
    """

    resource: ServiceResource

    def __init__(self):
        super().__init__()

    def _custom_error(self, name: str) -> str:
        # Ensure we never pass a name starting with 'States.' to CustomErrorName
        if name.startswith("States."):
            return name[len("States."):].lstrip(".")
        return name

    def from_state_props(self, state_props: StateProps) -> None:
        super().from_state_props(state_props=state_props)
        # Validate the HTTP integration is supported on program creation.
        self._validate_http_integration()

    def _validate_http_integration(self):
        """Validate that the HTTP integration is properly configured."""
        if self.resource.api_action.lower() != "invoke":
            raise ValueError(
                f"The HTTP task action '{self.resource.api_action}' is not supported. "
                "Only 'invoke' is supported for HTTP tasks."
            )

        # Validate integration patterns
        if self.resource.condition and self.resource.condition not in _SUPPORTED_INTEGRATION_PATTERNS:
            raise ValueError(
                f"HTTP tasks do not support the integration pattern '{self.resource.condition}'. "
                f"Supported patterns: {list(_SUPPORTED_INTEGRATION_PATTERNS)} or synchronous."
            )

    def _get_supported_parameters(self) -> set[str] | None:
        """Return the set of supported parameters for the HTTP invoke action."""
        return _SUPPORTED_API_PARAM_BINDINGS.get(self.resource.api_action.lower())

    def _get_sfn_resource(self) -> str:
        """Return the resource identifier for Step Functions history events."""
        return self.resource.api_action

    def _get_sfn_resource_type(self) -> str:
        """Return the resource type for Step Functions history events."""
        return "http"

    def _build_url(self, base_url: str, query_parameters: Optional[Dict[str, Any]] = None) -> str:
        """
        Build the complete URL with query parameters if provided.

        Args:
            base_url: The base URL
            query_parameters: Optional dictionary of query parameters

        Returns:
            Complete URL with query parameters appended
        """
        if not query_parameters:
            return base_url

        # Convert all values to strings and handle lists
        processed_params = {}
        for key, value in query_parameters.items():
            if isinstance(value, list):
                # For lists, use the same key multiple times
                processed_params[key] = [str(v) for v in value]
            else:
                processed_params[key] = str(value)

        # Build query string
        query_string = urlencode(processed_params, doseq=True)

        # Append to URL
        separator = "&" if "?" in base_url else "?"
        return f"{base_url}{separator}{query_string}"

    def _prepare_headers(self, headers: Optional[Dict[str, Any]] = None) -> Dict[str, str]:
        """
        Prepare HTTP headers for the request.

        Args:
            headers: Optional dictionary of headers

        Returns:
            Processed headers dictionary
        """
        processed_headers = {}

        if headers:
            for key, value in headers.items():
                if isinstance(value, list):
                    # Join list values with comma (standard HTTP header format)
                    processed_headers[key] = ", ".join(str(v) for v in value)
                else:
                    processed_headers[key] = str(value)

        return processed_headers

    def _prepare_auth(self, auth_config: Optional[Dict[str, Any]] = None) -> Optional[Any]:
        """
        Prepare authentication for the request.

        Args:
            auth_config: Optional authentication configuration

        Returns:
            Authentication object for requests library or None
        """
        if not auth_config:
            return None

        auth_type = auth_config.get("Type", "").lower()

        if auth_type == "basic":
            username = auth_config.get("Username", "")
            password = auth_config.get("Password", "")
            return HTTPBasicAuth(username, password)
        elif auth_type == "bearer":
            # Bearer token should be added to headers
            return None  # Will be handled in headers
        elif auth_type == "api_key":
            # API key should be added to headers or query params
            return None  # Will be handled separately
        else:
            LOG.warning(f"Unsupported authentication type: {auth_type}")
            return None

    def _add_auth_headers(
        self,
        headers: Dict[str, str],
        auth_config: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Add authentication headers if needed.

        Args:
            headers: Headers dictionary to modify
            auth_config: Optional authentication configuration
        """
        if not auth_config:
            return

        auth_type = auth_config.get("Type", "").lower()

        if auth_type == "bearer":
            token = auth_config.get("Token", "")
            headers["Authorization"] = f"Bearer {token}"
        elif auth_type == "api_key":
            key_name = auth_config.get("KeyName", "X-API-Key")
            key_value = auth_config.get("KeyValue", "")
            headers[key_name] = key_value

    def _send_eventbridge_event(
        self,
        env: Environment,
        eventbridge_config: Dict[str, Any],
        http_request_details: Dict[str, Any],
        task_token: Optional[str] = None
    ) -> None:
        """Send an event to EventBridge with HTTP request details and optional task token."""
        try:
            # Get the events store for the current account and region
            account_id = env.aws_execution_details.account
            region = env.aws_execution_details.region
            store = events_stores[account_id][region]
            
            # Get EventBridge configuration
            source = eventbridge_config.get("Source", "http.stepfunctions")
            detail_type = eventbridge_config.get("DetailType", "HTTP Task Event")
            event_bus_name = eventbridge_config.get("EventBusName", "default")
            
            # Build event detail
            event_detail = {
                "httpRequest": http_request_details,
                "executionArn": env.states.context_object.context_object_data["Execution"]["Id"],
                "stateMachineArn": env.states.context_object.context_object_data["StateMachine"]["Id"],
                "timestamp": timestamp(format=TIMESTAMP_FORMAT_TZ)
            }
            
            # Add custom detail from EventBridge config
            if "Detail" in eventbridge_config:
                if isinstance(eventbridge_config["Detail"], dict):
                    event_detail.update(eventbridge_config["Detail"])
                else:
                    event_detail["customDetail"] = eventbridge_config["Detail"]
            
            # Add task token if provided (for wait task token pattern)
            if task_token:
                event_detail["taskToken"] = task_token
            
            event_entry = {
                "Source": source,
                "DetailType": detail_type,
                "Detail": json.dumps(event_detail),
                "EventBusName": event_bus_name,
                "Time": timestamp(format=TIMESTAMP_FORMAT_TZ)
            }
            
            # Format the event using LocalStack's internal formatting
            formatted_event = format_event(
                event_entry,
                region=region,
                account_id=account_id,
                event_bus_name=event_bus_name
            )
            
            # Get the event bus
            event_bus = store.event_buses.get(event_bus_name, store.event_buses.get("default"))
            if not event_bus:
                LOG.warning(f"Event bus '{event_bus_name}' not found, creating default")

                event_bus_service = EventBusService.create_event_bus_service(
                    "default", region, account_id, None, None, None
                )
                store.event_buses["default"] = event_bus_service.event_bus
                event_bus = event_bus_service.event_bus

            events_provider = EventsProvider()
            
            # Use the internal method to process the formatted event
            if hasattr(events_provider, '_process_rules'):
                # Try to use the internal rule processing
                if configured_rules := list(event_bus.rules.values()):
                    for rule in configured_rules:
                        events_provider._process_rules(
                            rule=rule,
                            region=region,
                            account_id=account_id,
                            event_formatted=formatted_event,
                            trace_header=None  # No trace header for internal events
                        )
            
            LOG.info(f"Sent HTTP task event to EventBridge: {source}")
            
        except Exception as e:
            LOG.error(f"Failed to send event to EventBridge: {e}")
            # For debugging, let's not raise the exception to see if HTTP task continues
            LOG.warning(f"Continuing HTTP task execution despite EventBridge error: {e}")
    
    def _wait_for_callback(
        self,
        env: Environment,
        task_token: str,
        timeout_seconds: int
    ) -> Dict[str, Any]:
        """Wait for EventBridge callback with task token."""
        # Get the callback endpoint from the callback pool manager
        callback_endpoint = env.callback_pool_manager.get(task_token)
        
        if not callback_endpoint:
            raise ValueError(f"No callback endpoint found for task token: {task_token}")
        
        # Wait for callback outcome
        outcome = callback_endpoint.wait(timeout=timeout_seconds)
        
        if outcome is None:
            raise Exception("Task timed out waiting for EventBridge callback")
        
        if isinstance(outcome, CallbackOutcomeSuccess):
            return json.loads(outcome.output)
        elif isinstance(outcome, CallbackOutcomeFailure):
            error_msg = f"EventBridge callback failed: {outcome.error}"
            if outcome.cause:
                error_msg += f" - {outcome.cause}"
            raise Exception(error_msg)
        elif isinstance(outcome, CallbackOutcomeTimedOut):
            raise Exception("EventBridge callback timed out")
        else:
            raise Exception(f"Unknown callback outcome type: {type(outcome)}")

    def _parse_response_body(self, response: requests.Response, transform: bool = True) -> Any:
        """
        Parse the response body based on content type and transform settings.

        Args:
            response: The HTTP response object
            transform: Whether to automatically parse JSON responses

        Returns:
            Parsed response body
        """
        content_type = response.headers.get("Content-Type", "").lower()

        if not response.content:
            return None

        if transform and "application/json" in content_type:
            try:
                return response.json()
            except json.JSONDecodeError:
                # If JSON parsing fails, return as string
                return response.text
        else:
            # Return as string for non-JSON or when transform is disabled
            return response.text

    def _verify_size_quota(self, env: Environment, value: Any) -> None:
        """Verify that the response is within Step Functions size limits."""
        value_str = to_json_str(value) if not isinstance(value, str) else value
        is_within: bool = is_within_size_quota(value_str)
        if is_within:
            return

        resource_type = self._get_sfn_resource_type()
        resource = self._get_sfn_resource()
        cause = (
            f"The state/task '{resource_type}' returned a result with a size "
            "exceeding the maximum number of bytes service limit."
        )
        raise FailureEventException(
            failure_event=FailureEvent(
                env=env,
                error_name=StatesErrorName(typ=StatesErrorNameType.StatesStatesDataLimitExceeded),
                event_type=HistoryEventType.TaskFailed,
                event_details=EventDetails(
                    taskFailedEventDetails=TaskFailedEventDetails(
                        error=StatesErrorNameType.StatesStatesDataLimitExceeded.to_name(),
                        cause=cause,
                        resourceType=resource_type,
                        resource=resource,
                    )
                ),
            )
        )

    def _from_error(self, env: Environment, ex: Exception) -> FailureEvent:
        """
        Convert an exception to a FailureEvent for Step Functions.

        Args:
            env: The execution environment
            ex: The exception that occurred

        Returns:
            FailureEvent describing the error
        """
        if isinstance(ex, requests.exceptions.RequestException):
            # Handle various requests exceptions
            if isinstance(ex, requests.exceptions.Timeout):
                err = "Http.Timeout"
                msg = "HTTP request timed out"
            elif isinstance(ex, requests.exceptions.ConnectionError):
                err = "Http.ConnectionError"
                msg = f"Failed to connect: {str(ex)}"
            elif isinstance(ex, requests.exceptions.HTTPError):
                err = "Http.HttpError"
                msg = f"HTTP protocol error: {str(ex)}"
            else:
                err = "Http.RequestFailed"
                msg = f"HTTP request failed: {str(ex)}"

            err = self._custom_error(err)

            return FailureEvent(
                env=env,
                error_name=CustomErrorName(error_name=err),
                event_type=HistoryEventType.TaskFailed,
                event_details=EventDetails(
                    taskFailedEventDetails=TaskFailedEventDetails(
                        error=err,
                        cause=msg,
                        resource=self._get_sfn_resource(),
                        resourceType=self._get_sfn_resource_type(),
                    )
                ),
            )

        return super()._from_error(env=env, ex=ex)

    def _get_timed_out_failure_event(self, env: Environment) -> FailureEvent:
        """Get timeout failure event specific to HTTP tasks."""
        return FailureEvent(
            env=env,
            error_name=StatesErrorName(typ=StatesErrorNameType.StatesTimeout),
            event_type=HistoryEventType.TaskTimedOut,
            event_details=EventDetails(
                taskTimedOutEventDetails=TaskTimedOutEventDetails(
                    resourceType=self._get_sfn_resource_type(),
                    resource=self._get_sfn_resource(),
                    error=StatesErrorNameType.StatesTimeout.to_name(),
                )
            ),
        )

    def _eval_http_task(
        self,
        env: Environment,
        resource_runtime_part: ResourceRuntimePart,
        parameters: dict,
        state_credentials: StateCredentials,
    ) -> None:
        """
        Execute the HTTP task with optional EventBridge integration.

        Args:
            env: The execution environment
            resource_runtime_part: Runtime information about the resource
            parameters: The task parameters
            state_credentials: Credentials for the task execution
        """
        # Extract parameters
        method = parameters.get("Method", "GET").upper()
        url = parameters.get("Url")
        eventbridge_config = parameters.get("EventBridge")
        
        if not url:
            raise FailureEventException(
                failure_event=FailureEvent(
                    env=env,
                    error_name=StatesErrorName(typ=StatesErrorNameType.StatesTaskFailed),
                    event_type=HistoryEventType.TaskFailed,
                    event_details=EventDetails(
                        taskFailedEventDetails=TaskFailedEventDetails(
                            error="States.Http.InvalidParameters",
                            cause="Url parameter is required for HTTP task",
                            resource=self._get_sfn_resource(),
                            resourceType=self._get_sfn_resource_type(),
                        )
                    ),
                )
            )

        # Prepare request components
        query_parameters = parameters.get("QueryParameters")
        headers = parameters.get("Headers")
        request_body = parameters.get("RequestBody")
        auth_config = parameters.get("Authentication")
        connection_config = parameters.get("ConnectionConfig", {})
        transform = parameters.get("Transform", True)
        
        # Check if this is a wait task token pattern
        is_wait_task_token = self.resource.condition == ResourceCondition.WaitForTaskToken
        task_token = None
        
        if is_wait_task_token:
            # Get task token from context for waitForTaskToken pattern
            task_token = env.states.context_object.context_object_data.get("Task", {}).get("Token")
            if not task_token:
                raise FailureEventException(
                    failure_event=FailureEvent(
                        env=env,
                        error_name=StatesErrorName(typ=StatesErrorNameType.StatesTaskFailed),
                        event_type=HistoryEventType.TaskFailed,
                        event_details=EventDetails(
                            taskFailedEventDetails=TaskFailedEventDetails(
                                error="States.Http.NoTaskToken",
                                cause="Task token is required for waitForTaskToken pattern",
                                resource=self._get_sfn_resource(),
                                resourceType=self._get_sfn_resource_type(),
                            )
                        ),
                    )
                )

        # Build complete URL
        full_url = self._build_url(url, query_parameters)

        # Prepare headers
        request_headers = self._prepare_headers(headers)

        # Add authentication headers if needed
        self._add_auth_headers(request_headers, auth_config)

        # Prepare authentication
        auth = self._prepare_auth(auth_config)

        # Get timeout values
        connect_timeout = connection_config.get("ConnectionTimeout", DEFAULT_CONNECTION_TIMEOUT)
        response_timeout = connection_config.get("ResponseTimeout", DEFAULT_RESPONSE_TIMEOUT)
        timeout = (connect_timeout, response_timeout)

        # Prepare request body
        request_data = None
        request_json = None

        if request_body is not None:
            if isinstance(request_body, (dict, list)):
                request_json = request_body
                # Ensure Content-Type is set for JSON
                if "Content-Type" not in request_headers:
                    request_headers["Content-Type"] = "application/json"
            else:
                request_data = str(request_body)
        
        # Send EventBridge event if configured (before HTTP request)
        if eventbridge_config:
            http_request_details = {
                "method": method,
                "url": full_url,
                "headers": dict(request_headers),
                "body": request_json if request_json else request_data,
                "timestamp": timestamp(format=TIMESTAMP_FORMAT_TZ)
            }
            
            self._send_eventbridge_event(
                env=env,
                eventbridge_config=eventbridge_config,
                http_request_details=http_request_details,
                task_token=task_token
            )
            
            # If this is a wait task token pattern, wait for callback instead of making HTTP request
            if is_wait_task_token:
                callback_timeout = connection_config.get("CallbackTimeout", 300)  # 5 minutes default
                callback_result = self._wait_for_callback(env, task_token, callback_timeout)
                env.stack.append(callback_result)
                return

        try:
            # Make the HTTP request
            response = requests.request(
                method=method,
                url=full_url,
                headers=request_headers,
                data=request_data,
                json=request_json,
                auth=auth,
                timeout=timeout,
                allow_redirects=True,
                verify=True  # Always verify SSL certificates
            )

            # Check for HTTP errors (4xx, 5xx)
            if not response.ok:
                # For non-2xx status codes, we should fail the task
                error_name = self._custom_error(f"Http.{response.status_code}")
                error_message = f"HTTP request returned status code {response.status_code}"

                raise FailureEventException(
                    failure_event=FailureEvent(
                        env=env,
                        error_name=CustomErrorName(error_name=error_name),
                        event_type=HistoryEventType.TaskFailed,
                        event_details=EventDetails(
                            taskFailedEventDetails=TaskFailedEventDetails(
                                error=error_name,
                                cause=error_message,
                                resource=self._get_sfn_resource(),
                                resourceType=self._get_sfn_resource_type(),
                            )
                        ),
                    )
                )

            # Build response object
            response_body = self._parse_response_body(response, transform)

            result = {
                "StatusCode": response.status_code,
                "StatusText": response.reason,
                "Headers": dict(response.headers),
                "ResponseBody": response_body,
            }

            # Push result to stack
            env.stack.append(result)

        except requests.exceptions.RequestException as ex:
            # Convert requests exceptions to Step Functions failures
            failure_event = self._from_error(env, ex)
            raise FailureEventException(failure_event=failure_event)
        except Exception as ex:
            # Handle any other unexpected exceptions
            LOG.error(f"Unexpected error in HTTP task: {ex}")
            failure_event = self._from_error(env, ex)
            raise FailureEventException(failure_event=failure_event)

    def _before_eval_execution(
        self,
        env: Environment,
        resource_runtime_part: ResourceRuntimePart,
        parameters: dict,
        state_credentials: StateCredentials,
    ) -> None:
        """Add scheduled and started events before task execution."""
        parameters_str = to_json_str(parameters)

        scheduled_event_details = TaskScheduledEventDetails(
            resource=self._get_sfn_resource(),
            resourceType=self._get_sfn_resource_type(),
            region=resource_runtime_part.region,
            parameters=parameters_str,
        )

        if not self.timeout.is_default_value():
            self.timeout.eval(env=env)
            timeout_seconds = env.stack.pop()
            scheduled_event_details["timeoutInSeconds"] = timeout_seconds

        if self.heartbeat is not None:
            self.heartbeat.eval(env=env)
            heartbeat_seconds = env.stack.pop()
            scheduled_event_details["heartbeatInSeconds"] = heartbeat_seconds

        if self.credentials:
            scheduled_event_details["taskCredentials"] = TaskCredentials(
                roleArn=state_credentials.role_arn
            )

        env.event_manager.add_event(
            context=env.event_history_context,
            event_type=HistoryEventType.TaskScheduled,
            event_details=EventDetails(taskScheduledEventDetails=scheduled_event_details),
        )

        env.event_manager.add_event(
            context=env.event_history_context,
            event_type=HistoryEventType.TaskStarted,
            event_details=EventDetails(
                taskStartedEventDetails=TaskStartedEventDetails(
                    resource=self._get_sfn_resource(),
                    resourceType=self._get_sfn_resource_type()
                )
            ),
        )

    def _after_eval_execution(
        self,
        env: Environment,
        resource_runtime_part: ResourceRuntimePart,
        parameters: dict,
        state_credentials: StateCredentials,
    ) -> None:
        """Add succeeded event after task execution."""
        output = env.stack[-1]
        self._verify_size_quota(env=env, value=output)

        env.event_manager.add_event(
            context=env.event_history_context,
            event_type=HistoryEventType.TaskSucceeded,
            event_details=EventDetails(
                taskSucceededEventDetails=TaskSucceededEventDetails(
                    resource=self._get_sfn_resource(),
                    resourceType=self._get_sfn_resource_type(),
                    output=to_json_str(output),
                    outputDetails=HistoryEventExecutionDataDetails(truncated=False),
                )
            ),
        )

    def _eval_execution(self, env: Environment) -> None:
        """
        Main execution method for the HTTP task.

        This overrides the ExecutionState's _eval_execution to implement
        HTTP-specific task execution logic with EventBridge integration.
        """
        # Handle wait task token pattern for EventBridge integration
        if self.resource.condition == ResourceCondition.WaitForTaskToken:
            # Generate a TaskToken uuid within the context object for callback
            task_token = env.states.context_object.update_task_token()
            env.callback_pool_manager.add(task_token)
            
        try:
            # Evaluate the resource to get runtime information
            self.resource.eval(env=env)
            resource_runtime_part: ResourceRuntimePart = env.stack.pop()

            # Get parameters and credentials
            parameters = self._eval_parameters(env=env)
            state_credentials = self._eval_state_credentials(env=env)

            # Add pre-execution events
            self._before_eval_execution(
                env=env,
                resource_runtime_part=resource_runtime_part,
                parameters=parameters,
                state_credentials=state_credentials,
            )

            # Check if we're in mocked mode
            if env.is_mocked_mode():
                mocked_response: MockedResponse = env.get_current_mocked_response()
                eval_mocked_response(env=env, mocked_response=mocked_response)
            else:
                # Execute the actual HTTP task
                self._eval_http_task(
                    env=env,
                    resource_runtime_part=resource_runtime_part,
                    parameters=parameters,
                    state_credentials=state_credentials,
                )

            # Add post-execution events
            self._after_eval_execution(
                env=env,
                resource_runtime_part=resource_runtime_part,
                parameters=parameters,
                state_credentials=state_credentials,
            )
            
        finally:
            # Ensure the TaskToken field is reset after execution
            env.states.context_object.context_object_data.pop("Task", None)