FROM python:3.11-slim-bookworm

WORKDIR /app
COPY . .
RUN pip install --no-cache-dir -r requirements.txt

ENV USER_DB_PATH=/tmp/user_db.sock

CMD ["python", "server.py"]
