# client_example.py
import asyncio
import json
import sys
import time
import websockets

SERVER_WS = "ws://localhost:8000/ws"

async def run_client(username: str, topic: str):
    async with websockets.connect(SERVER_WS) as ws:
        # Send initial join payload
        join_payload = {"username": username, "topic": topic}
        await ws.send(json.dumps(join_payload))
        # Receive join ack
        resp = await ws.recv()
        print("Server:", resp)

        # Start a task to listen for incoming messages
        async def listener():
            try:
                while True:
                    text = await ws.recv()
                    print("INCOMING:", text)
            except Exception as e:
                print("Listener stopped:", e)

        listener_task = asyncio.create_task(listener())

        # Interact: send a few messages, request /list, then exit
        await asyncio.sleep(1)
        # Send a JSON chat message
        await ws.send(json.dumps({"message": "Hello everyone!"}))
        await asyncio.sleep(1)
        # Send plain text message
        await ws.send("This is a plain text message")
        await asyncio.sleep(1)
        # Ask server for /list (active topics)
        await ws.send("/list")
        await asyncio.sleep(2)

        # send another message then exit
        await ws.send(json.dumps({"message": f"Goodbye from {username} at {int(time.time())}"}))
        await asyncio.sleep(1)

        await ws.close()
        await listener_task

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python client_example.py <username> <topic>")
        print("Example: python client_example.py alice sports")
        sys.exit(1)

    uname = sys.argv[1]
    topic = sys.argv[2]
    asyncio.run(run_client(uname, topic))
