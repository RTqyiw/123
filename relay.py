import asyncio
import websockets
import json
import datetime
import os

PORT = int(os.environ.get("PORT", 8765))

# channel -> set of websockets
CHANNELS = {}

def get_channel(ws):
    for ch, members in CHANNELS.items():
        if ws in members:
            return ch
    return None

async def broadcast_peers(channel):
    members = CHANNELS.get(channel, set())
    payload = json.dumps({"peers": len(members)})
    dead = []
    for client in list(members):
        try:
            await client.send(payload)
        except Exception:
            dead.append(client)
    for c in dead:
        members.discard(c)

async def handler(websocket):
    channel = None
    nick = "?"
    addr = websocket.remote_address

    try:
        # Первое сообщение должно быть {"join": "channel", "nick": "name"}
        raw = await asyncio.wait_for(websocket.recv(), timeout=10.0)
        data = json.loads(raw)
        channel = data.get("join", "").strip()
        nick    = data.get("nick", "?").strip()

        if not channel:
            await websocket.close()
            return

        if channel not in CHANNELS:
            CHANNELS[channel] = set()
        CHANNELS[channel].add(websocket)

        ts = datetime.datetime.now().strftime("%H:%M:%S")
        print(f"[{ts}] [{channel}] +{nick} ({addr})  members={len(CHANNELS[channel])}", flush=True)
        await broadcast_peers(channel)

        async for raw in websocket:
            try:
                data = json.loads(raw)
            except Exception:
                data = {"msg": raw}

            msg       = data.get("msg", "")
            raw_field = data.get("raw", "")

            if raw_field:
                forward = json.dumps({"raw": raw_field})
                ts = datetime.datetime.now().strftime("%H:%M:%S")
                print(f"[{ts}] [{channel}] {nick} >> [RAW] {raw_field[:60]}", flush=True)
            elif msg:
                forward = json.dumps({"msg": msg})
                ts = datetime.datetime.now().strftime("%H:%M:%S")
                print(f"[{ts}] [{channel}] {nick} >> {msg}", flush=True)
            else:
                continue

            members = CHANNELS.get(channel, set())
            dead = []
            for client in list(members):
                if client is websocket:
                    continue
                try:
                    await client.send(forward)
                except Exception:
                    dead.append(client)
            for c in dead:
                members.discard(c)

    except asyncio.TimeoutError:
        pass
    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        if channel and channel in CHANNELS:
            CHANNELS[channel].discard(websocket)
            if not CHANNELS[channel]:
                del CHANNELS[channel]
            else:
                await broadcast_peers(channel)
        ts = datetime.datetime.now().strftime("%H:%M:%S")
        print(f"[{ts}] [{channel}] -{nick} ({addr})", flush=True)

async def main():
    print(f"CheckSim Relay starting on port {PORT}", flush=True)
    async with websockets.serve(handler, "0.0.0.0", PORT, ping_interval=20, ping_timeout=10):
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
