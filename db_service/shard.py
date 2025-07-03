import asyncio
import aiofiles
import mmap
import os
import time
import json
import logging
from pathlib import Path
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
LOG_LIMIT = 1000
RECORD_SIZE = sum(MAX_LENGTHS[f] for f in FIELDS) + (len(FIELDS) - 1)
TTL_SECONDS = 5 * 3600  # 5 hours
IO_TIMEOUT = 5.0

# заранее считаем смещения полей внутри записи
FIELD_OFFSETS: Dict[str, int] = {}
_off = 0
for i, f in enumerate(FIELDS):
    FIELD_OFFSETS[f] = _off
    _off += MAX_LENGTHS[f] + (1 if i < len(FIELDS) - 1 else 0)

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s')

class ShardManager:
    def __init__(self, base_path: str = 'shards'):
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)
        if not os.access(self.base_path, os.R_OK | os.W_OK):
            raise OSError(f'No read/write permission for {self.base_path}')
            
        self.meta_lock = asyncio.Lock()
        self.data_lock = asyncio.Lock()
        self.cache_lock = asyncio.Lock()
        
        # cache: ref -> Record
        self.cache: Dict[str, Record] = {}

    @staticmethod
    def _sanitize_shard_prefix(username: str) -> str:
        return username[0].lower() if username and username[0].isalpha() else 'q'

    @staticmethod
    def _clean_field(value: str) -> str:
        return value.replace('\n', '').replace('|', '')

    @staticmethod
    def _validate_ref(ref: str) -> (str, int):
        try:
            shard, rec_id = ref.split(':')
            return shard, int(rec_id)
        except ValueError:
            raise ValueError(f"Invalid reference format: {ref}")

    def _info_path(self, prefix: str) -> Path:
        return self.base_path / f'{prefix}.info'

    def _data_path(self, prefix: str, rec_id: int) -> Path:
        return self.base_path / f'{prefix}{rec_id}'

    
    #def _get_shard_info_path(self, prefix: str) -> str:
    #    return os.path.join(self.base_path, f'{prefix}.info')

    
    #def _get_shard_data_path(self, prefix: str, index: int) -> str:
    #    return os.path.join(self.base_path, f'{prefix}{index}')

    
    async def _load_info(self, prefix: str) -> Dict:
        path = self._info_path(prefix)
        if not path.exists():
            return {'shards': 0, 'free': {}, 'log': []}

        async with aiofiles.open(path, 'r') as f:
            try:
                raw = await asyncio.wait_for(f.read(), IO_TIMEOUT)
                data = json.loads(raw) if raw.strip() else {}
            except Exception as e:
                logger.error(f'Error reading {path}: {e}')
                data = {}

        return {
            'shards': data.get('shards', 0),
            'free'  : data.get('free', {}),
            'log'   : data.get('log',  []),
        }

    
    async def _save_info(self, prefix: str, info: Dict):
        path = self._info_path(prefix)
        tmp  = path.with_suffix('.tmp')
        async with aiofiles.open(tmp, 'w') as f:
            await f.write(json.dumps(info, indent=2))
            await f.flush()
        os.replace(tmp, path)
        os.chmod(path, 0o644)


    async def add_record(self, username: str, password_hash: str,
                         ip_reg: str, last_logged: str, last_ip: str) -> str:
        if not username or not username[0].isalpha():
            raise ValueError('Username must start with a letter')

        cleaned = {
            'username'     : self._clean_field(username)[:MAX_LENGTHS['username']],
            'password_hash': self._clean_field(password_hash)[:MAX_LENGTHS['password_hash']],
            'ip_reg'       : self._clean_field(ip_reg)[:MAX_LENGTHS['ip_reg']],
            'last_logged'  : self._clean_field(last_logged)[:MAX_LENGTHS['last_logged']],
            'last_ip'      : self._clean_field(last_ip)[:MAX_LENGTHS['last_ip']],
        }

        prefix = self._sanitize_shard_prefix(cleaned['username'])
        now    = int(time.time())

        async with self.meta_lock, self.data_lock:
            info = await self._load_info(prefix)
            shard_idx = info['shards'] - 1 if info['shards'] else 0
            data_path = self._data_path(prefix, shard_idx)
            free_list = info['free'].setdefault(str(shard_idx), [])

            # берем свободный слот либо идём в конец
            if free_list:
                rec_id = free_list.pop(0)
            else:
                if data_path.exists():
                    async with aiofiles.open(data_path, 'rb') as f:
                        await f.seek(0, os.SEEK_END)
                        rec_id = await f.tell() // RECORD_SIZE
                else:
                    rec_id = 0

            # если лимит переполнен — новый шард
            if rec_id >= RECORD_LIMIT:
                shard_idx += 1
                info['shards'] = shard_idx + 1
                rec_id = 0
            elif info['shards'] == 0:
                info['shards'] = 1

            # формируем запись
            values = [str(rec_id), *[cleaned[f] for f in FIELDS if f != 'id']]
            blob   = FIELD_SEPARATOR.join(
                        v.ljust(MAX_LENGTHS[k]).encode()
                        for k, v in zip(FIELDS, values))

            # пишем
            async with aiofiles.open(
                    data_path, 'r+b' if data_path.exists() else 'wb') as f:
                await f.seek(rec_id * RECORD_SIZE)
                await f.write(blob)
                await f.flush()

            ref = f'{prefix}{shard_idx}:{rec_id}'
            info['log'].append((now, f'CREATE {ref}'))
            info['log'] = info['log'][-LOG_LIMIT:]
            await self._save_info(prefix, info)

        # кэш
        async with self.cache_lock:
            self.cache[ref] = Record(fields=dict(zip(FIELDS, values)),
                                     timestamp=now)
        return ref


    async def get_record(self, ref: str,
                         fields: Optional[List[str]] = None) -> Optional[Dict[str, str]]:
        async with self.cache_lock:
            if ref in self.cache:
                rec = self.cache[ref]
                return {k: rec.fields[k] for k in (fields or FIELDS)}

        try:
            shard, rec_id = self._validate_ref(ref)
            prefix, idx   = shard[:-1], int(shard[-1])
        except ValueError:
            return None

        path = self._data_path(prefix, idx)
        if not path.exists():
            return None

        async with aiofiles.open(path, 'rb') as f:
            await f.seek(rec_id * RECORD_SIZE)
            raw = await f.read(RECORD_SIZE)
        if len(raw) != RECORD_SIZE:
            return None

        parts  = raw.split(FIELD_SEPARATOR)
        values = [p.decode().strip() for p in parts]
        record = dict(zip(FIELDS, values))

        async with self.cache_lock:
            self.cache[ref] = Record(fields=record, timestamp=int(time.time()))
        return {k: record[k] for k in (fields or FIELDS)}

    async def delete_record(self, ref: str) -> bool:
        try:
            shard, rec_id = self._validate_ref(ref)
            prefix, idx   = shard[:-1], int(shard[-1])
        except ValueError:
            return False

        if not await self.get_record(ref):      # проверяем существование
            return False

        now  = int(time.time())
        path = self._data_path(prefix, idx)

        async with self.meta_lock, self.data_lock:
            try:
                async with aiofiles.open(path, 'r+b') as f:
                    await f.seek(rec_id * RECORD_SIZE)
                    await f.write(b'\x00' * RECORD_SIZE)
                    await f.flush()
            except FileNotFoundError:
                return False

            info = await self._load_info(prefix)
            info['free'].setdefault(str(idx), []).append(rec_id)
            info['log'].append((now, f'DELETE {ref}'))
            info['log'] = info['log'][-LOG_LIMIT:]
            await self._save_info(prefix, info)

        async with self.cache_lock:
            self.cache.pop(ref, None)
        return True

    async def update_record(self, ref: str, updates: Dict[str, str]) -> bool:
        orig = await self.get_record(ref)
        if not orig:
            return False

        cleaned = {k: self._clean_field(v)[:MAX_LENGTHS[k]]
                   for k, v in updates.items()
                   if k in MAX_LENGTHS and k != 'id'}

        if not cleaned:
            return True

        shard, rec_id = self._validate_ref(ref)
        prefix, idx   = shard[:-1], int(shard[-1])
        path          = self._data_path(prefix, idx)

        async with self.data_lock:
            # читаем текущую запись
            async with aiofiles.open(path, 'r+b') as f:
                await f.seek(rec_id * RECORD_SIZE)
                raw = await f.read(RECORD_SIZE)
                pats = raw.split(FIELD_SEPARATOR)
            fields_now = {k: pats[i].decode().rstrip()
                          for i, k in enumerate(FIELDS)}
            fields_now.update(cleaned)
            fields_now['id'] = str(rec_id)

            blob = FIELD_SEPARATOR.join(
                       fields_now[k].ljust(MAX_LENGTHS[k]).encode()
                       for k in FIELDS)

            async with aiofiles.open(path, 'r+b') as f:
                await f.seek(rec_id * RECORD_SIZE)
                await f.write(blob)
                await f.flush()

        async with self.cache_lock:
            self.cache.pop(ref, None)   # проще удалить, чем аккуратно патчить
        return True


    # ────────── mmap-поиск
    def _scan_file_sync(self, path: Path, field: str, value: str) -> List[str]:
        """Синхронно сканирует один файл и возвращает список ref-ов."""
        refs   = []
        offset = FIELD_OFFSETS[field]
        length = MAX_LENGTHS[field]
        prefix_shard = path.name         # например  d0
        with path.open('rb') as f, mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
            total = mm.size() // RECORD_SIZE
            for rec_id in range(total):
                pos = rec_id * RECORD_SIZE + offset
                chunk = mm[pos: pos + length].rstrip()
                if chunk.decode() == value:
                    refs.append(f'{prefix_shard}:{rec_id}')
        return refs


    async def find_records(self, field: str, value: str,
                           fields: Optional[List[str]] = None) -> List[Dict[str, str]]:
        if field not in FIELDS:
            raise ValueError(f'Invalid field: {field}')

        # собираем все файлы-шарды — это всё, что НЕ заканчивается на .info
        shard_paths = [p for p in self.base_path.iterdir()
                       if p.is_file() and not p.name.endswith('.info')]

        # параллельно сканируем
        tasks = [asyncio.to_thread(self._scan_file_sync, p, field, value)
                 for p in shard_paths]
        refs_nested = await asyncio.gather(*tasks)
        refs = [r for sub in refs_nested for r in sub]

        # получаем записи (можно параллельно, но чаще нужно ≤ десятка)
        records = []
        for ref in refs:
            rec = await self.get_record(ref, fields)
            if rec is not None:
                records.append({'ref': ref, **rec})
        return records
