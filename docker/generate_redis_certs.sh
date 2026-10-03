#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"
mkdir -p certs
cd certs

# 1) Приватный ключ собственного CA (корневого удостоверяющего центра)
openssl genrsa -out ca.key 4096

# 2) Самоподписанный сертификат CA на 10 лет.
#    Extensions обязательны: современный OpenSSL (Python 3.13+) отвергает CA
#    без basicConstraints=CA:TRUE и keyUsage=keyCertSign (RFC 5280).
openssl req -x509 -new -nodes -sha256 -days 3650 \
  -key ca.key \
  -subj "/CN=redis-ca" \
  -addext "basicConstraints=critical,CA:TRUE" \
  -addext "keyUsage=critical,keyCertSign,cRLSign" \
  -out ca.crt

# 3) Приватный ключ сервера Redis
openssl genrsa -out redis.key 2048

# 4) CSR (запрос на подпись) для сервера, с SAN на localhost и имя сервиса в compose
openssl req -new -key redis.key \
  -subj "/CN=redis" \
  -addext "subjectAltName=DNS:redis,DNS:redis-pubsub,DNS:localhost,IP:127.0.0.1" \
  -out redis.csr

# 5) Подписываем CSR нашим CA → получаем серверный сертификат на 1 год.
#    Расширения: SAN (имена хостов) + keyUsage/extendedKeyUsage=serverAuth.
openssl x509 -req -in redis.csr \
  -CA ca.crt -CAkey ca.key -CAcreateserial \
  -days 365 -sha256 \
  -extfile <(printf "subjectAltName=DNS:redis,DNS:redis-pubsub,DNS:localhost,IP:127.0.0.1\nbasicConstraints=CA:FALSE\nkeyUsage=critical,digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth") \
  -out redis.crt

# 6) Приводим права: ключи — только владельцу
chmod 600 ca.key redis.key
chmod 644 ca.crt redis.crt

# CSR больше не нужен
rm redis.csr