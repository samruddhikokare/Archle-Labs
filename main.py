# main.py
import asyncio
import json
import logging
import time
import uuid
from typing import Dict, List

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
import uvicorn

app = FastAPI()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# In-memory state:
# topics: {
#   topic_name: {
#       "users": {username: websocket},
#       "messages": [{ "id": str, "username": str, "message": str, "timestamp": int }]
#   }
# }
topics: Dict[str, Dict] = {}
topics_lock = asyncio.Lock()  # protect topics structure when necessary

MESSAGE_EXPIRY_SECONDS = 30


async def ensure_unique_username(topic: str, base_username: str) -> str:
    """Make username unique within topic by appending #n if needed."""
    async with topics_lock:
        if topic not in topics:
            return base_username
        existing = set(topics[topic]["users"].keys())
    if base_username not in existing:
        return base_username

    # find next suffix
    i = 2
    while f"{base_username}#{i}" in existing:
        i += 1
    return f"{base_username}#{i}"


async def add_user_to_topic(topic: str, username: str, websocket: WebSocket):
    async with topics_lock:
        if topic not in topics:
            topics[topic] = {"users": {}, "messages": []}
        topics[topic]["users"][username] = websocket
        logging.info(f"User '{username}' joined topic '{topic}' (users now: {len(topics[topic]['users'])})")


async def remove_user_from_topic(topic: str, username: str):
    async with topics_lock:
        if topic in topics:
            users = topics[topic]["users"]
            if username in users:
                users.pop(username, None)
                logging.info(f"User '{username}' removed from topic '{topic}'")
            # If topic becomes empty, remove it entirely
            if not users:
                topics.pop(topic, None)
                logging.info(f"Topic '{topic}' removed because it became empty")


def make_message_payload(username: str, message: str) -> Dict:
    return {
        "username": username,
        "message": message,
        "timestamp": int(time.time())
    }


async def expire_message(topic: str, message_id: str, delay: int = MESSAGE_EXPIRY_SECONDS):
    """Delete message from topic after `delay` seconds."""
    await asyncio.sleep(delay)
    async with topics_lock:
        topic_entry = topics.get(topic)
        if not topic_entry:
            return
        before = len(topic_entry["messages"])
        topic_entry["messages"] = [m for m in topic_entry["messages"] if m["id"] != message_id]
        after = len(topic_entry["messages"])
        if before != after:
            logging.info(f"Expired message {message_id} in topic '{topic}' (messages {before}->{after})")


async def broadcast_to_topic(topic: str, payload: Dict, exclude_username: str = None):
    """Send JSON payload to every user in topic except exclude_username."""
    async with topics_lock:
        topic_entry = topics.get(topic, None)
        if not topic_entry:
            return
        recipients = [(u, ws) for u, ws in topic_entry["users"].items() if u != exclude_username]

    text = json.dumps(payload)
    # send without holding topics_lock
    for username, ws in recipients:
        try:
            await ws.send_text(text)
        except Exception as e:
            logging.warning(f"Failed to send to {username} in topic {topic}: {e}")


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    client_address = websocket.client.host if websocket.client else "unknown"
    logging.info(f"New websocket connection accepted from {client_address}")

    username = None
    topic = None

    try:
        # Expect initial JSON payload to join: {"username": "...", "topic": "..."}
        text = await websocket.receive_text()
        try:
            data = json.loads(text)
            requested_username = data.get("username")
            requested_topic = data.get("topic")
            if not isinstance(requested_username, str) or not isinstance(requested_topic, str):
                raise ValueError("username and topic must be strings")
        except Exception:
            # Invalid initial payload: inform client and close
            await websocket.send_text(json.dumps({"error": "Invalid initial payload. Send JSON: {\"username\":\"alice\",\"topic\":\"sports\"}"}))
            logging.warning(f"Invalid initial payload from {client_address}: {text}")
            await websocket.close()
            return

        # Ensure unique username in topic
        username = await ensure_unique_username(requested_topic, requested_username)
        topic = requested_topic

        # Add to topic
        await add_user_to_topic(topic, username, websocket)

        # Acknowledge join to this user
        await websocket.send_text(json.dumps({"status": "joined", "username": username, "topic": topic}))

        # Notify others in topic that user joined (optional)
        join_notice = {"system": True, "message": f"{username} joined the topic", "timestamp": int(time.time())}
        await broadcast_to_topic(topic, join_notice, exclude_username=username)

        # Main loop to receive messages from client
        while True:
            msg_text = await websocket.receive_text()
            # Special command: /list
            if msg_text.strip() == "/list":
                # Build active topic list with counts
                async with topics_lock:
                    lines = []
                    for tname, tentry in topics.items():
                        lines.append(f"{tname} ({len(tentry['users'])} users)")
                payload = {"active_topics": lines}
                # respond only to the requesting user
                await websocket.send_text(json.dumps(payload))
                continue

            # Try to parse JSON message payload: {"message": "Hello world"}
            is_json = False
            try:
                j = json.loads(msg_text)
                if isinstance(j, dict) and "message" in j:
                    is_json = True
                    body_message = str(j["message"])
                else:
                    # If it's JSON but doesn't contain 'message', treat as error
                    await websocket.send_text(json.dumps({"error": "JSON payload must contain 'message' field"}))
                    continue
            except Exception:
                is_json = False

            if not is_json:
                # Unexpected format - treat as text message content
                body_message = msg_text

            # Build message object
            message_id = str(uuid.uuid4())
            payload = make_message_payload(username, body_message)
            payload["id"] = message_id

            # Store message in topic messages list
            async with topics_lock:
                if topic in topics:
                    topics[topic]["messages"].append(payload)

            # Broadcast to other users in topic
            await broadcast_to_topic(topic, payload, exclude_username=username)

            # Send delivery ack back to sender
            ack = {"status": "delivered", "message_id": message_id, "timestamp": int(time.time())}
            await websocket.send_text(json.dumps(ack))

            # Schedule message expiry
            asyncio.create_task(expire_message(topic, message_id, MESSAGE_EXPIRY_SECONDS))

    except WebSocketDisconnect:
        logging.info(f"WebSocketDisconnect for user '{username}' in topic '{topic}' from {client_address}")
    except Exception as e:
        logging.exception(f"Error in websocket handler for {client_address}: {e}")
    finally:
        # cleanup
        if topic and username:
            await remove_user_from_topic(topic, username)
            # Notify others in topic that user left
            leave_notice = {"system": True, "message": f"{username} left the topic", "timestamp": int(time.time())}
            await broadcast_to_topic(topic, leave_notice, exclude_username=None)


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False)
