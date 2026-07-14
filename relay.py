import asyncio
import websockets
import json
import datetime
import os

PORT = int(os.environ.get("PORT", 8765))

# channel -> set of websockets
CHANNELS = {}

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

async def leave_channel(ws, channel, nick):
    if channel and channel in CHANNELS:
        CHANNELS[channel].discard(ws)
        if not CHANNELS[channel]:
            del CHANNELS[channel]
        else:
            await broadcast_peers(channel)
        ts = datetime.datetime.now().strftime("%H:%M:%S")
        print(f"[{ts}] [{channel}] -{nick}", flush=True)

async def handler(websocket):
    channel = None
    nick = "?"
    addr = websocket.remote_address

    try:
        async for raw in websocket:
            try:
                data = json.loads(raw)
            except Exception:
                data = {"msg": raw}

            # join — смена/вход в канал
            if "join" in data:
                new_channel = data.get("join", "").strip()
                new_nick    = data.get("nick", "?").strip()
                if not new_channel:
                    continue

                # Выходим из старого канала
                if channel:
                    await leave_channel(websocket, channel, nick)

                channel = new_channel
                nick    = new_nick

                if channel not in CHANNELS:
                    CHANNELS[channel] = set()
                CHANNELS[channel].add(websocket)

                ts = datetime.datetime.now().strftime("%H:%M:%S")
                print(f"[{ts}] [{channel}] +{nick} ({addr})  members={len(CHANNELS[channel])}", flush=True)
                await broadcast_peers(channel)
                continue

            # Нет канала — игнорируем
            if not channel:
                continue

            msg       = data.get("msg", "")
            raw_field = data.get("raw", "")
            quit      = data.get("quit", False)
            inv_field = data.get("inv", "")

            if quit:
                forward = json.dumps({"quit": True})
                ts = datetime.datetime.now().strftime("%H:%M:%S")
                print(f"[{ts}] [{channel}] {nick} >> [QUIT]", flush=True)
            elif inv_field:
                forward = json.dumps({"inv": inv_field})
                ts = datetime.datetime.now().strftime("%H:%M:%S")
                print(f"[{ts}] [{channel}] {nick} >> [INV] {len(inv_field)} chars", flush=True)
            elif raw_field:
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

    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        await leave_channel(websocket, channel, nick)

async def main():
    print(f"CheckSim Relay starting on port {PORT}", flush=True)
    async with websockets.serve(handler, "0.0.0.0", PORT, ping_interval=20, ping_timeout=10):
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
