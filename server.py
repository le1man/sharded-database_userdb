import asyncio
import os
import logging
import json

from shard import ShardManager

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(message)s')

SOCKET_PATH = os.getenv("USER_DB_PATH", "/tmp/user_db.sock")

async def handle_client(reader: asyncio.StreamReader,
                        writer: asyncio.StreamWriter,
                        manager: ShardManager):
    addr = writer.get_extra_info('peername')
    logging.info(f"Client connected: {addr}")

    try:
        while not reader.at_eof():
            data = await reader.readline()
            if not data:
                break
            req = data.decode().strip()

            try:
                if req.startswith("CREATE "):
                    _, u, ph, ip1, t, ip2 = req.split(" ", 5)
                    ref = await manager.add_record(u, ph, ip1, t, ip2)
                    writer.write(f"OK {ref}\n".encode())

                elif req.startswith("GET "):
                    parts = req.split(" ", 2)
                    ref = parts[1]
                    fields = parts[2].split(',') if len(parts) == 3 else None
                    rec = await manager.get_record(ref, fields)
                    if rec is None:
                        writer.write(f"ERROR Not found\n".encode())
                    else:
                        writer.write(f"OK {json.dumps(rec)}\n".encode())

                elif req.startswith("DELETE "):
                    ref = req.split(" ",1)[1]
                    ok = await manager.delete_record(ref)
                    writer.write(( "OK Deleted\n" if ok else f"ERROR Delete failed\n").encode())

                elif req.startswith("UPDATE "):
                    parts = req.split(" ",2)
                    ref = parts[1]
                    upd = dict(pair.split("=",1) for pair in parts[2].split())
                    ok = await manager.update_record(ref, upd)
                    writer.write(( "OK Updated\n" if ok else f"ERROR Update failed\n").encode())

                elif req.startswith("FIND "):
                    parts = req.split(" ",3)
                    field, value = parts[1], parts[2]
                    fields = parts[3].split(',') if len(parts)==4 else None
                    recs = await manager.find_records(field, value, fields)
                    writer.write(f"OK {json.dumps(recs)}\n".encode())

                else:
                    writer.write(b"ERROR UNKNOWN COMMAND\n")

            except Exception as e:
                writer.write(f"ERROR {e}\n".encode())

            await writer.drain()
    finally:
        writer.close()
        await writer.wait_closed()
        logging.info(f"Connection closed: {addr}")

async def start_server():
    if os.path.exists(SOCKET_PATH):
        os.remove(SOCKET_PATH)

    mgr = ShardManager(base_path='shards')
    server = await asyncio.start_unix_server(
        lambda r, w: handle_client(r, w, mgr),
        path=SOCKET_PATH
    )
    logging.info(f"Server listening on {SOCKET_PATH}")
    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    try:
        asyncio.run(start_server())
    except KeyboardInterrupt:
        logging.info("Shutting down.")
    finally:
        if os.path.exists(SOCKET_PATH):
            os.remove(SOCKET_PATH)
