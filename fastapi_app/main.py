import os
import asyncio
import json
from fastapi import FastAPI, HTTPException, Depends, status, Query, Path
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from pydantic import BaseModel

# --- Configs from envs ---
ADMIN_LOGIN = os.getenv("ADMIN_LOGIN")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD")
SOCKET_PATH = os.getenv("USER_DB_PATH", "/tmp/user_db.sock")

if not ADMIN_LOGIN or not ADMIN_PASSWORD:
    raise RuntimeError("Необходимо задать ADMIN_LOGIN и ADMIN_PASSWORD в окружении")

# --- FastAPI + BasicAuth ---
app = FastAPI(title="UserDB Proxy API")
security = HTTPBasic()


def verify_credentials(credentials: HTTPBasicCredentials = Depends(security)):
    correct_user = credentials.username == ADMIN_LOGIN
    correct_pass = credentials.password == ADMIN_PASSWORD
    if not (correct_user and correct_pass):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials",
            headers={"WWW-Authenticate": "Basic"},
        )


# --- Pydantic-models ---
class CreateRequest(BaseModel):
    username: str
    password_hash: str
    ip_reg: str
    last_logged: str
    last_ip: str


class UpdateRequest(BaseModel):
    # any of the fields except id
    username: str | None = None
    password_hash: str | None = None
    ip_reg: str | None = None
    last_logged: str | None = None
    last_ip: str | None = None


# --- Helper function for communicating with a UNIX socket ---
async def send_cmd(cmd: str) -> str:
    try:
        reader, writer = await asyncio.open_unix_connection(SOCKET_PATH)
    except FileNotFoundError:
        raise HTTPException(status_code=500, detail="Socket server is not running")
    # we send a command
    writer.write((cmd + "\n").encode())
    await writer.drain()
    # read the answer
    raw = await reader.readline()
    writer.close()
    await writer.wait_closed()
    if not raw:
        raise HTTPException(status_code=500, detail="No response from server")
    text = raw.decode().strip()
    if text.startswith("OK "):
        return text[3:]
    elif text == "OK Deleted" or text == "OK Updated":
        return text[3:]  # just "Deleted"/"Updated"
    else:
        # ERROR ...
        raise HTTPException(status_code=400, detail=text)


# --- endpoints ---

@app.post("/records", dependencies=[Depends(verify_credentials)])
async def create_record(req: CreateRequest):
    cmd = f"CREATE {req.username} {req.password_hash} {req.ip_reg} {req.last_logged} {req.last_ip}"
    res = await send_cmd(cmd)
    # res == "<ref>"
    return {"ref": res}


@app.get("/records/{ref}", dependencies=[Depends(verify_credentials)])
async def get_record(
        ref: str = Path(..., description="Reference, e.g. a0:3"),
        fields: str | None = Query(
            None,
            description="Comma-separated list of fields to return, e.g. username,last_ip"
        )
):
    if fields:
        cmd = f"GET {ref} {fields}"
    else:
        cmd = f"GET {ref}"
    res = await send_cmd(cmd)
    # res == JSON-string with object
    return json.loads(res)


@app.put("/records/{ref}", dependencies=[Depends(verify_credentials)])
async def update_record(
        ref: str = Path(..., description="Reference, e.g. a0:3"),
        req: UpdateRequest = Depends()
):
    pairs = []
    for k, v in req.dict().items():
        if v is not None:
            pairs.append(f"{k}={v}")
    if not pairs:
        raise HTTPException(status_code=400, detail="No fields to update")
    cmd = f"UPDATE {ref} " + " ".join(pairs)
    res = await send_cmd(cmd)  # "Updated"
    return {"status": res}


@app.delete("/records/{ref}", dependencies=[Depends(verify_credentials)])
async def delete_record(ref: str = Path(..., description="Reference, e.g. a0:3")):
    cmd = f"DELETE {ref}"
    res = await send_cmd(cmd)  # "Deleted"
    return {"status": res}


@app.get("/find", dependencies=[Depends(verify_credentials)])
async def find_records(
        field: str = Query(..., description="Поле для поиска, например username"),
        value: str = Query(..., description="Значение поля"),
        fields: str | None = Query(
            None,
            description="Комма-сепарейтед список полей для вывода"
        )
):
    if fields:
        cmd = f"FIND {field} {value} {fields}"
    else:
        cmd = f"FIND {field} {value}"
    res = await send_cmd(cmd)
    # res == JSON string with list of records
    return json.loads(res)
