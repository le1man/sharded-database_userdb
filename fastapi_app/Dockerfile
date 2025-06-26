FROM python:3.11-slim-bookworm

ARG ADMIN_LOGIN
ARG ADMIN_PASSWORD

# Можно использовать значения аргументов
ENV ADMIN_LOGIN=${ADMIN_LOGIN}
ENV ADMIN_PASSWORD=${ADMIN_PASSWORD}

WORKDIR /app
COPY . .
RUN pip install --no-cache-dir -r requirements.txt

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "1818"]
