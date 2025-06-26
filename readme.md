### Инструкция по запуску

Установка зависимостей

```bash
python3 -m venv venv
source venv/bin/activate
pip install fastapi uvicorn
```

Задать перменные окружения для доступа апи

```bash
export ADMIN_LOGIN=your_admin
export ADMIN_PASSWORD=supersecret
```

# (необязательно) если UNIX-сокет не /tmp/user_db.sock:

```bash
export USER_DB_PATH=/path/to/your.sock
```

Запускаем Unix сокет сервер шард бд

# В отдельном терминале

```bash
cd path/to/db_service
python server.py
```

Запустить FastAPI-оболочку (порт можно указать любой)

```bash
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

## Тестирование
Открыть http://localhost:8000/docs — там будет Swagger UI.

Пример с curl:

# CREATE
```bash
curl -u your_admin:supersecret \
  -X POST "http://localhost:8000/records" \
  -H "Content-Type: application/json" \
  -d '{"username":"alice","password_hash":"hash","ip_reg":"127.0.0.1","last_logged":"2025-06-16T12:00:00","last_ip":"127.0.0.1"}'
```

# GET
```bash
curl -u your_admin:supersecret "http://localhost:8000/records/a0:0"
```

# UPDATE
```bash
curl -u your_admin:supersecret \
  -X PUT "http://localhost:8000/records/a0:0" \
  -H "Content-Type: application/json" \
  -d '{"last_ip":"192.168.0.2"}'
```

# FIND
```bash
curl -u your_admin:supersecret "http://localhost:8000/find?field=username&value=alice"
```

# DELETE
```bash
curl -u your_admin:supersecret -X DELETE "http://localhost:8000/records/a0:0"
```