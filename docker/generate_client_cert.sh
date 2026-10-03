#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"
mkdir -p certs

cd certs

openssl genrsa -out client.key 2048

# 8) CSR клиента. CN можно сделать равным имени ACL-пользователя — удобно для аудита.
openssl req -new -key client.key \
  -subj "/CN=redis-client" \
  -out client.csr

openssl x509 -req -in client.csr \
  -CA ca.crt -CAkey ca.key -CAcreateserial \
  -days 365 -sha256 \
  -extfile <(printf "basicConstraints=CA:FALSE\nkeyUsage=critical,digitalSignature\nextendedKeyUsage=clientAuth") \
  -out client.crt

chmod 600 client.key
chmod 644 client.crt
rm client.csr