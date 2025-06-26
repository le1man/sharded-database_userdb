import asyncio
import json
import os

# Path to UNIX-socket
SOCKET_PATH = os.getenv("USER_DB_PATH", "/tmp/user_db.sock")


async def run_tests():
    reader, writer = await asyncio.open_unix_connection(SOCKET_PATH)

    # 1) CREATE
    cmd = "CREATE alice secret_hash 127.0.0.1 2025-06-16T12:00:00 127.0.0.1"
    print("> " + cmd)
    writer.write((cmd + "\\n").encode())
    await writer.drain()
    resp = await reader.readline()
    text = resp.decode().strip()
    print("< " + text)
    if not text.startswith("OK "):
        print("CREATE failed, aborting")
        writer.close()
        await writer.wait_closed()
        return
    ref = text.split(" ", 1)[1]

    # 2) GET
    cmd = f"GET {ref}"
    print("> " + cmd)
    writer.write((cmd + "\\n").encode())
    await writer.drain()
    resp = await reader.readline()
    print("< " + resp.decode().strip())

    # 3) UPDATE
    cmd = f"UPDATE {ref} last_ip=192.168.0.2"
    print("> " + cmd)
    writer.write((cmd + "\\n").encode())
    await writer.drain()
    resp = await reader.readline()
    print("< " + resp.decode().strip())

    # 4) GET before UPDATE (only last_ip)
    cmd = f"GET {ref} last_ip"
    print("> " + cmd)
    writer.write((cmd + "\\n").encode())
    await writer.drain()
    resp = await reader.readline()
    print("< " + resp.decode().strip())

    # 5) FIND by username
    cmd = "FIND username alice"
    print("> " + cmd)
    writer.write((cmd + "\\n").encode())
    await writer.drain()
    resp = await reader.readline()
    print("< " + resp.decode().strip())

    # 6) DELETE
    cmd = f"DELETE {ref}"
    print("> " + cmd)
    writer.write((cmd + "\\n").encode())
    await writer.drain()
    resp = await reader.readline()
    print("< " + resp.decode().strip())

    # 7) GET before DELETE
    cmd = f"GET {ref}"
    print("> " + cmd)
    writer.write((cmd + "\\n").encode())
    await writer.drain()
    resp = await reader.readline()
    print("< " + resp.decode().strip())

    writer.close()
    await writer.wait_closed()


if __name__ == "__main__":
    asyncio.run(run_tests())
