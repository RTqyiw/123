import asyncio
import websockets
import json
import datetime
import os

PORT = int(os.environ.get("PORT", 8765))

# канал -> список подключений
CHANNELS = {}


def now():
    return datetime.datetime.now().strftime("%H:%M:%S")


def log(msg):
    print(f"[{now()}] {msg}", flush=True)


def channel_info(channel):
    count = len(CHANNELS.get(channel, set()))
    return f"канал '{channel}' [{count} подкл.]"


async def broadcast_peers(channel):
    members = CHANNELS.get(channel, set())
    payload = json.dumps({"peers": len(members)})

    dead = []

    for client in list(members):
        try:
            await client.send(payload)
        except Exception:
            dead.append(client)

    for client in dead:
        members.discard(client)


async def leave_channel(ws, channel, nick):
    if channel and channel in CHANNELS:
        CHANNELS[channel].discard(ws)

        if not CHANNELS[channel]:
            del CHANNELS[channel]
            log(f"Выход: {nick} | {channel_info(channel)} (канал удалён)")
        else:
            await broadcast_peers(channel)
            log(f"Выход: {nick} | {channel_info(channel)}")


async def handler(websocket):
    channel = None
    nick = "?"

    addr = (
        websocket.remote_address[0]
        if websocket.remote_address
        else "?"
    )

    try:
        async for raw in websocket:

            try:
                data = json.loads(raw)
            except Exception:
                data = {"msg": raw}


            # вход в канал
            if "join" in data:

                new_channel = data.get("join", "").strip()
                new_nick = data.get("nick", "?").strip()

                if not new_channel:
                    continue


                if channel:
                    await leave_channel(
                        websocket,
                        channel,
                        nick
                    )


                channel = new_channel
                nick = new_nick


                if channel not in CHANNELS:
                    CHANNELS[channel] = set()


                CHANNELS[channel].add(websocket)


                count = len(CHANNELS[channel])

                status = (
                    "готово к работе"
                    if count >= 2
                    else "ожидание второго ПК"
                )


                log(
                    f"Подключение: {nick} "
                    f"({addr}) | "
                    f"{channel_info(channel)} | "
                    f"{status}"
                )


                await broadcast_peers(channel)

                continue



            if not channel:
                continue



            msg = data.get("msg", "")
            raw_field = data.get("raw", "")
            quit_flag = data.get("quit", False)
            inv_field = data.get("inv", "")



            if quit_flag:

                forward = json.dumps({
                    "quit": True
                })

                log(
                    f"Выход с сервера: "
                    f"{nick} | {channel_info(channel)}"
                )


            elif inv_field:

                forward = json.dumps({
                    "inv": inv_field
                })

                log(
                    f"Инвентарь: "
                    f"{nick} -> "
                    f"{channel_info(channel)}"
                )


            elif raw_field:

                forward = json.dumps({
                    "raw": raw_field
                })

                preview = raw_field[:50].replace(
                    "\n",
                    " "
                )

                log(
                    f"Служебное: "
                    f"{nick} -> "
                    f"{channel_info(channel)} | "
                    f"{preview}"
                )


            elif msg:

                forward = json.dumps({
                    "msg": msg
                })

                log(
                    f"Сообщение: "
                    f"{nick} -> "
                    f"{channel_info(channel)} | "
                    f"{msg[:60]}"
                )


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



            for client in dead:
                members.discard(client)



    except websockets.exceptions.ConnectionClosed:
        pass


    finally:
        await leave_channel(
            websocket,
            channel,
            nick
        )



async def main():

    print("=" * 50, flush=True)
    print("  CheckSim Relay Сервер", flush=True)
    print(f"  Порт: {PORT}", flush=True)
    print("=" * 50, flush=True)


    async with websockets.serve(
        handler,
        "0.0.0.0",
        PORT,
        ping_interval=20,
        ping_timeout=10
    ):

        log(
            "Сервер запущен, "
            "ожидаю подключений..."
        )

        await asyncio.Future()



if __name__ == "__main__":
    asyncio.run(main())
