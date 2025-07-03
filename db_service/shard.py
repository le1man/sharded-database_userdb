import asyncio
import aiofiles
import os
import time
import json
import logging
from typing import Optional, List, Dict

from record import Record

# Settings
RECORD_LIMIT = 5000
FIELD_SEPARATOR = b'|'
FIELDS = ['id', 'username', 'password_hash', 'ip_reg', 'last_logged', 'last_ip']
MAX_LENGTHS = {
    'id': 10,
    'username': 24,
    'password_hash': 64,
    'ip_reg': 16,
    'last_logged': 19,
    'last_ip': 16,
}
LOG_LIMIT = 100
RECORD_SIZE = sum(MAX_LENGTHS[f] for f in FIELDS) + (len(FIELDS) - 1)
TTL_SECONDS = 5 * 3600  # 5 hours
IO_TIMEOUT = 5.0

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s')

class ShardManager:
    def __init__(self, base_path: str = 'shards'):
        self.base_path = base_path
        self.meta_lock = asyncio.Lock()
        self.data_lock = asyncio.Lock()
        self.cache_lock = asyncio.Lock()
        # cache: ref -> Record
        self.cache: Dict[str, Record] = {}

        os.makedirs(self.base_path, exist_ok=True)
        if not os.access(self.base_path, os.R_OK | os.W_OK):
            raise OSError(f"No read/write permissions for {self.base_path}")

    @staticmethod
    def _sanitize_shard_prefix(username: str) -> str:
        return username[0].lower() if username and username[0].isalpha() else 'q'

    @staticmethod
    def _clean_field(value: str) -> str:
        return value.replace('\n', '').replace('|', '')

    @staticmethod
    def _validate_ref(ref: str) -> (str, int):
        try:
            shard, idx = ref.split(':')
            return shard, int(idx)
        except ValueError:
            raise ValueError(f"Invalid reference format: {ref}")

    def _get_shard_info_path(self, prefix: str) -> str:
        return os.path.join(self.base_path, f'{prefix}.info')

    def _get_shard_data_path(self, prefix: str, index: int) -> str:
        return os.path.join(self.base_path, f'{prefix}{index}')

    async def _load_info(self, prefix: str) -> Dict:
        path = self._get_shard_info_path(prefix)
        if not os.path.exists(path):
            return {'shards': 0, 'free': {}, 'log': [], 'index': {}}

        try:
            async with aiofiles.open(path, 'r') as f:
                content = await asyncio.wait_for(f.read(), timeout=IO_TIMEOUT)
        except Exception as e:
            logger.error(f"Error reading info {path}: {e}")
            return {'shards': 0, 'free': {}, 'log': [], 'index': {}}

        try:
            data = json.loads(content) if content.strip() else {}
        except json.JSONDecodeError:
            logger.error(f"Invalid JSON in {path}")
            data = {}

        # Проверяем структуру
        if not isinstance(data, dict):
            data = {}
        return {
            'shards': data.get('shards', 0),
            'free': data.get('free', {}),
            'log': data.get('log', []),
            'index': data.get('index', {})
        }

    async def _save_info(self, prefix: str, info: Dict):
        path = self._get_shard_info_path(prefix)
        tmp = path + '.tmp'
        # Recording via aiofiles
        try:
            async with aiofiles.open(tmp, 'w') as f:
                await f.write(json.dumps(info, indent=2))
                await f.flush()
        except Exception as e:
            logger.error(f"Error writing temp info {tmp}: {e}")
            if os.path.exists(tmp):
                os.remove(tmp)
            raise
        # Atomic replace
        os.replace(tmp, path)
        os.chmod(path, 0o644)


    async def add_record(self, username: str, password_hash: str, ip_reg: str,
                         last_logged: str, last_ip: str) -> str:
        if not username or not username[0].isalpha():
            raise ValueError("Username must start with a letter")

        cleaned = {
            'username': self._clean_field(username),
            'password_hash': self._clean_field(password_hash),
            'ip_reg': self._clean_field(ip_reg),
            'last_logged': self._clean_field(last_logged),
            'last_ip': self._clean_field(last_ip),
        }
        # Length validation
        for k, v in cleaned.items():
            if len(v) > MAX_LENGTHS[k]:
                raise ValueError(f"{k} too long")

        prefix = self._sanitize_shard_prefix(cleaned['username'])

        now = int(time.time())
        async with self.meta_lock:
            async with self.data_lock:
                info = await self._load_info(prefix)
                shard_index = info['shards'] - 1 if info['shards'] else 0
                data_path = self._get_shard_data_path(prefix, shard_index)
                free = info['free'].setdefault(str(shard_index), [])

                if free:
                    record_id = free.pop(0)
                else:
                    # we count records
                    if os.path.exists(data_path):
                        async with aiofiles.open(data_path, 'rb') as f:
                            await f.seek(0, os.SEEK_END)
                            count = await f.tell() // RECORD_SIZE
                            record_id = count
                    else:
                        record_id = 0

                if record_id >= RECORD_LIMIT:
                    shard_index += 1
                    info['shards'] = shard_index + 1
                    record_id = 0
                elif info['shards'] == 0:
                    info['shards'] = 1

                # Collecting the record bytes
                parts = []
                values = [str(record_id), *[cleaned[f] for f in FIELDS if f!='id']]
                for k, v in zip(FIELDS, values):
                    encoded = v.ljust(MAX_LENGTHS[k]).encode()
                    parts.append(encoded)
                record_bytes = FIELD_SEPARATOR.join(parts)

                # Write
                async with aiofiles.open(data_path, 'r+b' if os.path.exists(data_path) else 'wb') as f:
                    await f.seek(record_id * RECORD_SIZE)
                    await f.write(record_bytes)
                    await f.flush()

                ref = f"{prefix}{shard_index}:{record_id}"
                # Updating info
                info['log'].append((now, f'CREATE {ref}'))
                info['log'] = info['log'][-LOG_LIMIT:]
                # indexing
                for k, v in zip(FIELDS, values):
                    key = f"{k}:{v}"
                    info['index'].setdefault(key, []).append(ref)

                await self._save_info(prefix, info)

        # Update cache
        rec = {k: v for k, v in zip(FIELDS, values)}
        async with self.cache_lock:
            self.cache[ref] = Record(fields=rec, timestamp=now)

        return ref

    async def get_record(self, ref: str, fields: Optional[List[str]] = None) -> Optional[Dict[str, str]]:
        async with self.cache_lock:
            if ref in self.cache:
                rec = self.cache[ref]
                return {k: rec.fields[k] for k in (fields or FIELDS)}

        try:
            shard, record_id = self._validate_ref(ref)
            prefix, shard_index = shard[:-1], int(shard[-1])
        except ValueError:
            return None

        data_path = self._get_shard_data_path(prefix, shard_index)
        if not os.path.exists(data_path):
            return None

        async with aiofiles.open(data_path, 'rb') as f:
            await f.seek(record_id * RECORD_SIZE)
            raw = await f.read(RECORD_SIZE)
        if len(raw) != RECORD_SIZE:
            return None

        parts = raw.split(FIELD_SEPARATOR)
        if len(parts) != len(FIELDS):
            return None
        values = [p.decode('utf-8', errors='ignore').strip() for p in parts]
        rec = dict(zip(FIELDS, values))
        now = int(time.time())
        async with self.cache_lock:
            self.cache[ref] = Record(fields=rec, timestamp=now)
        return {k: rec[k] for k in (fields or FIELDS)}

    async def delete_record(self, ref: str) -> bool:
        try:
            shard, record_id = self._validate_ref(ref)
            prefix, shard_index = shard[:-1], int(shard[-1])
        except ValueError:
            return False

        now = int(time.time())
        # Getting old fields to remove from index
        old = await self.get_record(ref)
        if not old:
            return False

        async with self.meta_lock:
            async with self.data_lock:
                data_path = self._get_shard_data_path(prefix, shard_index)
                try:
                    async with aiofiles.open(data_path, 'r+b') as f:
                        await f.seek(record_id * RECORD_SIZE)
                        await f.write(b'\x00' * RECORD_SIZE)
                        await f.flush()
                except FileNotFoundError:
                    return False

                info = await self._load_info(prefix)
                info['free'].setdefault(str(shard_index), []).append(record_id)
                info['log'].append((now, f'DELETE {ref}'))
                info['log'] = info['log'][-LOG_LIMIT:]
                # deletion from index
                for k, v in old.items():
                    key = f"{k}:{v}"
                    if key in info['index'] and ref in info['index'][key]:
                        info['index'][key].remove(ref)
                await self._save_info(prefix, info)

        async with self.cache_lock:
            if ref in self.cache:
                del self.cache[ref]

        return True

    async def update_record(self, ref: str, updates: Dict[str, str]) -> bool:
        old = await self.get_record(ref)
        if not old:
            return False

        # We trim and clean
        cleaned = {}
        for k, v in updates.items():
            if k == 'id' or k not in MAX_LENGTHS:
                continue
            v2 = self._clean_field(v)[:MAX_LENGTHS[k]]
            cleaned[k] = v2

        for v in cleaned.values():
            if not isinstance(v, str):
                raise ValueError("All updates must be strings")

        try:
            shard, record_id = self._validate_ref(ref)
            prefix, shard_index = shard[:-1], int(shard[-1])
        except ValueError:
            return False

        now = int(time.time())
        async with self.meta_lock:
            async with self.data_lock:
                # We get the full current record
                full = await self.get_record(ref)
                for k, v in cleaned.items():
                    full[k] = v
                # id field
                full['id'] = str(record_id)

                # Let's write again
                parts = []
                for k in FIELDS:
                    encoded = full[k].ljust(MAX_LENGTHS[k]).encode()
                    parts.append(encoded)
                data = FIELD_SEPARATOR.join(parts)

                data_path = self._get_shard_data_path(prefix, shard_index)
                async with aiofiles.open(data_path, 'r+b') as f:
                    await f.seek(record_id * RECORD_SIZE)
                    await f.write(data)
                    await f.flush()

                info = await self._load_info(prefix)
                info['log'].append((now, f'UPDATE {ref}'))
                info['log'] = info['log'][-LOG_LIMIT:]
                # updating index
                for k, new in cleaned.items():
                    old_val = old[k]
                    key_old = f"{k}:{old_val}"
                    key_new = f"{k}:{new}"
                    if key_old in info['index'] and ref in info['index'][key_old]:
                        info['index'][key_old].remove(ref)
                    info['index'].setdefault(key_new, []).append(ref)

                await self._save_info(prefix, info)

        async with self.cache_lock:
            updated = self.cache.get(ref)
            if updated:
                updated.fields.update(cleaned)
                self.cache[ref] = Record(fields=updated.fields, timestamp=now)

        return True

    async def find_records(self, field: str, value: str,
                           fields: Optional[List[str]] = None) -> List[Dict[str, str]]:
        if field not in FIELDS:
            raise ValueError(f"Invalid field: {field}")

        key = f"{field}:{value}"
        async with self.meta_lock:
            info = await self._load_info(self._sanitize_shard_prefix(value))
            refs = info.get('index', {}).get(key, []).copy()

        results = []
        for ref in refs:
            rec = await self.get_record(ref, fields)
            if rec is not None:
                results.append({'ref': ref, **rec})
        return results
