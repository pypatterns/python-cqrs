"""
Example: FastAPI Integration with Server-Sent Events (SSE) and Streaming Handler

This example demonstrates how to integrate StreamingRequestMediator with FastAPI
using Server-Sent Events (SSE) to stream results to clients in real-time.

Use case: Processing file uploads or batch operations with real-time progress updates.
The client receives progress updates as each item is processed, and events are
handled in parallel for better performance.

================================================================================
HOW TO RUN THIS EXAMPLE
================================================================================

Option 1: Run Server Only (Interactive via Swagger UI)
-------------------------------------------------------
1. Start the server:
   python examples/streaming/fastapi_sse_streaming.py

2. Open your browser and go to:
   http://localhost:8000/docs

3. Find the POST /process-files endpoint and click "Try it out"

4. Enter request body:
   {
     "file_ids": ["file1", "file2", "file3"],
     "operation": "analyze"
   }

5. Click "Execute" to see the streaming response

Option 2: Run Server + Client (Full Example)
---------------------------------------------
Terminal 1 - Start the server:
   python examples/streaming/fastapi_sse_streaming.py

Terminal 2 - Run the client:
   python examples/streaming/fastapi_sse_streaming.py client

The client will:
- Connect to the server
- Send a request to process files
- Receive and log all SSE events in real-time
- Display progress updates as files are processed

Option 3: Use curl to test SSE endpoint
----------------------------------------
Start the server first, then run:
   curl -N -X POST http://localhost:8000/process-files \
     -H "Content-Type: application/json" \
     -d '{"file_ids": ["file1", "file2", "file3"], "operation": "analyze"}'

The -N flag disables buffering so you'll see events as they arrive.

Option 4: Use Python requests library
---------------------------------------
   import requests

   response = requests.post(
       "http://localhost:8000/process-files",
       json={"file_ids": ["file1", "file2"], "operation": "analyze"},
       stream=True
   )

   for line in response.iter_lines():
       if line:
           print(line.decode('utf-8'))

================================================================================
WHAT THIS EXAMPLE DEMONSTRATES
================================================================================

1. Streaming Request Handler:
   - Processes files one by one using StreamingRequestHandler
   - Yields results as each file is processed
   - Emits multiple domain events for each processed file

2. Parallel Event Processing:
   - Events are processed in parallel (max_concurrent_event_handlers=3)
   - Multiple event handlers run simultaneously:
     * FileProcessedEventHandler - logs processing
     * FileAnalyticsEventHandler - updates analytics
     * FileNotificationEventHandler - sends notifications

3. Server-Sent Events (SSE):
   - Real-time streaming of results to clients
   - Progress updates sent as each file is processed
   - Client receives events as they happen

4. SSE Client Implementation:
   - Example client that connects to SSE endpoint
   - Parses and handles different event types
   - Logs all events with formatted output

================================================================================
REQUIREMENTS
================================================================================

Make sure you have installed:
   - fastapi
   - uvicorn
   - aiohttp (for the client example)
   - cqrs (this package)
   - di (dependency injection)

================================================================================
"""

import asyncio
import json
import logging
import typing
from datetime import datetime

import aiohttp
import di
import fastapi
import pydantic
import uvicorn

import cqrs
from cqrs.message_brokers import devnull
from cqrs.requests import bootstrap

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# FastAPI app
app = fastapi.FastAPI(
    title="CQRS Streaming Handler with SSE",
    description="Example of streaming request handler with Server-Sent Events",
    version="1.0.0",
)

# ============================================================================
# Domain Models
# ============================================================================


class ProcessFilesCommand(cqrs.Request):
    """Command to process a batch of files."""

    file_ids: list[str] = pydantic.Field(description="List of file IDs to process")
    operation: str = pydantic.Field(
        default="analyze",
        description="Operation to perform: analyze, compress, or validate",
    )


class FileProcessedResult(cqrs.Response):
    """Response for each processed file."""

    file_id: str = pydantic.Field()
    status: str = pydantic.Field()
    processed_at: datetime = pydantic.Field()
    file_size_mb: float = pydantic.Field()
    operation: str = pydantic.Field()
    metadata: dict[str, typing.Any] = pydantic.Field(default_factory=dict)


# ============================================================================
# Domain Events
# ============================================================================


class FileProcessedEvent(cqrs.DomainEvent, frozen=True):
    """Event emitted when a file is processed."""

    file_id: str
    operation: str
    file_size_mb: float
    processed_at: datetime
    success: bool


class FileAnalyticsEvent(cqrs.DomainEvent, frozen=True):
    """Event for analytics tracking."""

    file_id: str
    operation: str
    file_size_mb: float
    processing_time_ms: int


class FileNotificationEvent(cqrs.DomainEvent, frozen=True):
    """Event for sending notifications."""

    file_id: str
    operation: str
    status: str
    message: str


# ============================================================================
# Request Handler (Streaming)
# ============================================================================


class ProcessFilesCommandHandler(
    cqrs.StreamingRequestHandler[ProcessFilesCommand, FileProcessedResult],
):
    """
    Streaming handler that processes files one by one.

    Simulates processing files (e.g., analyzing, compressing, validating)
    and yields results as each file is processed.
    """

    def __init__(self):
        self._events: list[cqrs.Event] = []

    @property
    def events(self) -> list[cqrs.Event]:
        """Return a copy of events collected during processing."""
        return self._events.copy()

    def clear_events(self) -> None:
        """Clear events after they have been processed and emitted."""
        self._events.clear()

    async def handle(
        self,
        request: ProcessFilesCommand,
    ) -> typing.AsyncIterator[FileProcessedResult]:
        """
        Process files one by one, yielding results after each file.

        For each file, multiple events are generated that will be processed
        in parallel by different event handlers.
        """
        logger.info(
            f"Starting to process {len(request.file_ids)} files " f"with operation: {request.operation}",
        )

        for idx, file_id in enumerate(request.file_ids):
            # Simulate file processing (e.g., reading, analyzing, etc.)
            # Different operations take different amounts of time
            if request.operation == "analyze":
                await asyncio.sleep(0.2)  # Analysis takes longer
            elif request.operation == "compress":
                await asyncio.sleep(0.15)
            else:  # validate
                await asyncio.sleep(0.1)

            # Mock file data
            file_size_mb = 10.0 + (idx * 2.5)
            processing_time_ms = int((0.1 + idx * 0.05) * 1000)

            # Create result
            result = FileProcessedResult(
                file_id=file_id,
                status="completed",
                processed_at=datetime.now(),
                file_size_mb=file_size_mb,
                operation=request.operation,
                metadata={
                    "processing_time_ms": processing_time_ms,
                    "items_found": idx * 10 + 5,
                },
            )

            # Emit multiple domain events that will be processed in parallel
            self._events.append(
                FileProcessedEvent(
                    file_id=file_id,
                    operation=request.operation,
                    file_size_mb=file_size_mb,
                    processed_at=datetime.now(),
                    success=True,
                ),
            )

            self._events.append(
                FileAnalyticsEvent(
                    file_id=file_id,
                    operation=request.operation,
                    file_size_mb=file_size_mb,
                    processing_time_ms=processing_time_ms,
                ),
            )

            self._events.append(
                FileNotificationEvent(
                    file_id=file_id,
                    operation=request.operation,
                    status="completed",
                    message=f"File {file_id} processed successfully",
                ),
            )

            logger.info(
                f"Processed file {file_id}, emitted {len(self._events)} events",
            )
            yield result


# ============================================================================
# Event Handlers
# ============================================================================


class FileProcessedEventHandler(cqrs.EventHandler[FileProcessedEvent]):
    """Handler for FileProcessedEvent - logs file processing."""

    async def handle(self, event: FileProcessedEvent) -> None:
        """Log file processing."""
        await asyncio.sleep(0.05)  # Simulate processing
        logger.info(
            f"📄 File {event.file_id} processed: " f"{event.operation} ({event.file_size_mb} MB)",
        )


class FileAnalyticsEventHandler(cqrs.EventHandler[FileAnalyticsEvent]):
    """Handler for FileAnalyticsEvent - updates analytics."""

    async def handle(self, event: FileAnalyticsEvent) -> None:
        """Update analytics."""
        await asyncio.sleep(0.03)  # Simulate database update
        logger.info(
            f"📊 Analytics updated for file {event.file_id}: " f"{event.processing_time_ms}ms processing time",
        )


class FileNotificationEventHandler(cqrs.EventHandler[FileNotificationEvent]):
    """Handler for FileNotificationEvent - sends notifications."""

    async def handle(self, event: FileNotificationEvent) -> None:
        """Send notification."""
        await asyncio.sleep(0.04)  # Simulate notification sending
        logger.info(f"🔔 Notification sent: {event.message}")


# ============================================================================
# Mappers
# ============================================================================


def commands_mapper(mapper: cqrs.RequestMap) -> None:
    """Map commands to handlers."""
    mapper.bind(ProcessFilesCommand, ProcessFilesCommandHandler)


def domain_events_mapper(mapper: cqrs.EventMap) -> None:
    """Map domain events to handlers."""
    mapper.bind(FileProcessedEvent, FileProcessedEventHandler)
    mapper.bind(FileAnalyticsEvent, FileAnalyticsEventHandler)
    mapper.bind(FileNotificationEvent, FileNotificationEventHandler)


# ============================================================================
# FastAPI Routes
# ============================================================================


def streaming_mediator_factory() -> cqrs.StreamingRequestMediator:
    """Factory function to create streaming mediator."""
    return bootstrap.bootstrap_streaming(
        di_container=di.Container(),
        commands_mapper=commands_mapper,
        domain_events_mapper=domain_events_mapper,
        message_broker=devnull.DevnullMessageBroker(),
        max_concurrent_event_handlers=3,  # Process up to 3 events in parallel
        concurrent_event_handle_enable=True,  # Enable parallel processing
    )


@app.post("/process-files", response_model=None)
async def process_files_stream(
    command: ProcessFilesCommand,
    mediator: cqrs.StreamingRequestMediator = fastapi.Depends(
        streaming_mediator_factory,
    ),
) -> fastapi.responses.StreamingResponse:
    """
    Process files using streaming handler and return results via SSE.

    This endpoint processes files one by one and streams results to the client
    using Server-Sent Events (SSE). Each file processing triggers multiple
    events that are handled in parallel.

    Example request:
        POST /process-files
        {
            "file_ids": ["file1", "file2", "file3"],
            "operation": "analyze"
        }
    """

    async def generate_sse():
        """Generate SSE events from streaming mediator."""
        try:
            # Send initial event
            yield f"data: {json.dumps({'type': 'start', 'message': f'Processing {len(command.file_ids)} files...'})}\n\n"

            processed_count = 0

            # Stream results from mediator
            async for result in mediator.stream(command):
                if result is None:
                    continue
                # Type assertion: we know this is FileProcessedResult from our handler
                result = typing.cast(FileProcessedResult, result)
                processed_count += 1

                # Format result as SSE event
                sse_data = {
                    "type": "progress",
                    "data": {
                        "file_id": result.file_id,
                        "status": result.status,
                        "processed_at": result.processed_at.isoformat(),
                        "file_size_mb": result.file_size_mb,
                        "operation": result.operation,
                        "metadata": result.metadata,
                        "progress": {
                            "current": processed_count,
                            "total": len(command.file_ids),
                            "percentage": int(
                                (processed_count / len(command.file_ids)) * 100,
                            ),
                        },
                    },
                }

                yield f"data: {json.dumps(sse_data)}\n\n"

                # Small delay to make streaming visible
                await asyncio.sleep(0.1)

            # Send completion event
            completion_data = {
                "type": "complete",
                "message": f"Successfully processed {processed_count} files",
                "total_processed": processed_count,
            }
            yield f"data: {json.dumps(completion_data)}\n\n"

        except Exception as e:
            error_data = {
                "type": "error",
                "message": str(e),
            }
            yield f"data: {json.dumps(error_data)}\n\n"
            logger.error(f"Error in SSE stream: {e}", exc_info=True)

    return fastapi.responses.StreamingResponse(
        generate_sse(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",  # Disable buffering in nginx
        },
    )


@app.get("/")
async def root():
    """Root endpoint with API information."""
    return {
        "message": "CQRS Streaming Handler with SSE Example",
        "endpoints": {
            "process_files": {
                "url": "/process-files",
                "method": "POST",
                "description": "Process files using streaming handler with SSE",
                "example_request": {
                    "file_ids": ["file1", "file2", "file3"],
                    "operation": "analyze",
                },
            },
            "docs": "/docs",
            "openapi": "/openapi.json",
        },
    }


# ============================================================================
# SSE Client
# ============================================================================


class SSEClient:
    """
    Client for consuming Server-Sent Events from the API.

    This client connects to the SSE endpoint and processes events in real-time,
    logging them as they arrive. Can be run in the background.
    """

    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url
        self.logger = logging.getLogger(f"{__name__}.SSEClient")

    async def process_files_stream(
        self,
        file_ids: list[str],
        operation: str = "analyze",
    ) -> None:
        """
        Connect to the /process-files endpoint and process SSE events.

        Args:
            file_ids: List of file IDs to process
            operation: Operation to perform (analyze, compress, or validate)
        """
        url = f"{self.base_url}/process-files"
        payload = {
            "file_ids": file_ids,
            "operation": operation,
        }

        self.logger.info(f"Connecting to {url}")
        self.logger.info(f"Request payload: {payload}")

        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload) as response:
                if response.status != 200:
                    error_text = await response.text()
                    self.logger.error(
                        f"Failed to connect: {response.status} - {error_text}",
                    )
                    return

                self.logger.info("Connected to SSE stream, receiving events...")
                self.logger.info("-" * 80)

                buffer = ""
                async for chunk in response.content.iter_any():
                    if not chunk:
                        break

                    buffer += chunk.decode("utf-8")

                    # Process complete SSE messages (ending with \n\n)
                    while "\n\n" in buffer:
                        message, buffer = buffer.split("\n\n", 1)
                        await self._process_sse_message(message)

                self.logger.info("-" * 80)
                self.logger.info("SSE stream closed")

    async def _process_sse_message(self, message: str) -> None:
        """Process a single SSE message."""
        if not message.strip():
            return

        # Parse SSE message format: "data: {...}"
        if message.startswith("data: "):
            data_str = message[6:]  # Remove "data: " prefix
            try:
                data = json.loads(data_str)
                await self._handle_event(data)
            except json.JSONDecodeError as e:
                self.logger.warning(f"Failed to parse SSE data: {e}")
        else:
            # Handle other SSE message types (event:, id:, etc.)
            self.logger.debug(f"Received SSE message: {message}")

    async def _handle_event(self, data: dict[str, typing.Any]) -> None:
        """Handle parsed SSE event data."""
        event_type = data.get("type", "unknown")

        if event_type == "start":
            self.logger.info(f"🚀 {data.get('message', 'Stream started')}")

        elif event_type == "progress":
            progress_data = data.get("data", {})
            progress = progress_data.get("progress", {})
            self.logger.info(
                f"📊 Progress: File {progress_data.get('file_id')} "
                f"({progress.get('current', 0)}/{progress.get('total', 0)}) - "
                f"{progress.get('percentage', 0)}% - "
                f"Status: {progress_data.get('status')} - "
                f"Size: {progress_data.get('file_size_mb', 0):.2f} MB",
            )

        elif event_type == "complete":
            self.logger.info(
                f"✅ {data.get('message', 'Processing completed')} - "
                f"Total processed: {data.get('total_processed', 0)}",
            )

        elif event_type == "error":
            self.logger.error(f"❌ Error: {data.get('message', 'Unknown error')}")

        else:
            self.logger.debug(f"Unknown event type: {event_type} - {data}")


async def run_client_example():
    """
    Example of using the SSE client to consume events from the API.

    This function demonstrates how to use the SSEClient to connect to the
    streaming endpoint and process events in real-time.
    """
    client = SSEClient()

    # Example: Process some files
    file_ids = ["file_001", "file_002", "file_003", "file_004"]

    logger.info("=" * 80)
    logger.info("SSE Client Example")
    logger.info("=" * 80)
    logger.info(f"Processing {len(file_ids)} files via SSE stream...")
    logger.info("")

    try:
        await client.process_files_stream(
            file_ids=file_ids,
            operation="analyze",
        )
    except aiohttp.ClientError as e:
        logger.error(f"Client error: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)

    logger.info("")
    logger.info("Client example completed")


# ============================================================================
# Main
# ============================================================================


if __name__ == "__main__":
    import sys

    # Check if running as client
    if len(sys.argv) > 1 and sys.argv[1] == "client":
        # Run as SSE client
        logger.info("Running as SSE client...")
        logger.info("Make sure the server is running on http://localhost:8000")
        logger.info("")
        asyncio.run(run_client_example())
    else:
        # Run as server
        logger.info("Starting FastAPI server with SSE streaming...")
        logger.info("Open http://localhost:8000/docs to try the API")
        logger.info("Use the /process-files endpoint to see streaming in action")
        logger.info("")
        logger.info("To run the client example:")
        logger.info("  python examples/streaming/fastapi_sse_streaming.py client")
        logger.info("")

        uvicorn.run(
            app,
            host="0.0.0.0",
            port=8000,
            log_level="info",
        )
