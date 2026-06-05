import os
import asyncio
import logging
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from sse_starlette.sse import EventSourceResponse
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("api-server")

app = FastAPI(title="CDC Pipeline API")

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Active client queues for broadcasting SSE events
listeners = set()

class CDCEvent(BaseModel):
    table: str
    operation: str
    timestamp: str

@app.post("/api/internal/cdc-event")
async def receive_cdc_event(event: CDCEvent):
    logger.info(f"Received internal CDC event: {event}")
    payload = {
        "table": event.table,
        "operation": event.operation,
        "timestamp": event.timestamp
    }
    # Distribute event to all active SSE listener queues
    for queue in list(listeners):
        try:
            await queue.put(payload)
        except Exception as e:
            logger.error(f"Error putting event to listener queue: {e}")
    return {"status": "success", "listeners_notified": len(listeners)}

@app.get("/api/cdc-stream")
async def cdc_stream(request: Request):
    logger.info("New client connected to SSE stream /api/cdc-stream")
    
    async def event_generator():
        queue = asyncio.Queue()
        listeners.add(queue)
        try:
            while True:
                # Terminate loop if client disconnects
                if await request.is_disconnected():
                    logger.info("SSE client disconnected.")
                    break
                try:
                    # Non-blocking wait for an event from the consumer
                    event_data = await asyncio.wait_for(queue.get(), timeout=1.0)
                    yield {
                        "event": "cdc_event",
                        "data": json.dumps(event_data)
                    }
                except asyncio.TimeoutError:
                    # Keep the connection alive
                    pass
        except asyncio.CancelledError:
            logger.info("SSE connection cancelled.")
        finally:
            listeners.discard(queue)

    return EventSourceResponse(event_generator())

import json

# Health check route
@app.get("/api/config")
async def get_config():
    try:
        path = "submission.json"
        if not os.path.exists(path):
            path = "../submission.json"
        with open(path, "r") as f:
            data = json.load(f)
        return data
    except Exception as e:
        logger.error(f"Failed to read submission.json: {e}")
        return {
            "apiBaseUrl": "http://localhost:8000",
            "searchIndexUrl": "http://localhost:7700",
            "searchIndexApiKey": "meili_master_key"
        }

@app.get("/api/health")
async def health():
    return {"status": "healthy"}

# Mount frontend static files if they exist
STATIC_DIR = "/app/static"
if os.path.exists(STATIC_DIR):
    logger.info(f"Mounting static files from {STATIC_DIR}")
    app.mount("/", StaticFiles(directory=STATIC_DIR, html=True), name="static")
else:
    logger.warning(f"Static directory {STATIC_DIR} not found. Running in API-only mode.")
