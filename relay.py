import asyncio
import websockets
import json
import datetime
import os

CLIENTS = set()
PORT = int(os.environ.get("PORT", 8765))

async def broadcast_peers():
    payload = json.dumps({"peers": len(CLIENTS)})
    snapshot = list(CLIENTS)
    dead = []
    for client in snapshot:
        try:
            await client.send(payload)
        except Exception:
            dead.append(client)
    for c in dead:
        CLIENTS.discard(c)

async def handler(websocket):
    CLIENTS.add(websocket)
    addr = websocket.remote_address
    print(f"[+] Connected: {addr}  (total: {len(CLIENTS)})", flush=True)
    await broadcast_peers()
    try:
        async for raw in websocket:
            try:
                data = json.loads(raw)
            except Exception:
                data = {"msg": raw}

            msg = data.get("msg", "")
            raw_field = data.get("raw", "")

            if raw_field:
                forward = json.dumps({"raw": raw_field})
                ts = datetime.datetime.now().strftime("%H:%M:%S")
                print(f"[{ts}] {addr} >> [RAW] {raw_field[:60]}", flush=True)
            elif msg:
                forward = json.dumps({"msg": msg})
                ts = datetime.datetime.now().strftime("%H:%M:%S")
                print(f"[{ts}] {addr} >> {msg}", flush=True)
            else:
                continue

            snapshot = list(CLIENTS)
            dead = []
            for client in snapshot:
                if client is websocket:
                    continue
                try:
                    await client.send(forward)
                except Exception:
                    dead.append(client)
            for c in dead:
                CLIENTS.discard(c)

    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        CLIENTS.discard(websocket)
        print(f"[-] Disconnected: {addr}  (total: {len(CLIENTS)})", flush=True)
        await broadcast_peers()

async def main():
    print(f"CheckSim Relay starting on port {PORT}", flush=True)
    # ping_interval=20 — держит соединение живым и не даёт Render засыпать
    async with websockets.serve(handler, "0.0.0.0", PORT, ping_interval=20, ping_timeout=10):
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
