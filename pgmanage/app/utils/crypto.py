import base64
import hashlib
import hmac
import os

from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from django.utils.crypto import pbkdf2


def encrypt(plaintext, key):
    iv = os.urandom(16)
    cipher = Cipher(algorithms.AES(key), modes.CFB(iv))
    encryptor = cipher.encryptor()
    ct = encryptor.update(plaintext.encode()) + encryptor.finalize()
    return base64.b64encode(iv + ct).decode()


def decrypt(encrypted_text, key):
    data = base64.b64decode(encrypted_text.encode())
    iv = data[:16]
    encrypted_key = data[16:]
    cipher = Cipher(algorithms.AES(key), modes.CFB(iv))
    decryptor = cipher.decryptor()
    conn_pass = decryptor.update(encrypted_key) + decryptor.finalize()

    return conn_pass.decode()


def make_hash(plaintext, current_user):
    iterations = 150000
    digest = hashlib.sha256
    salt = f"{current_user.date_joined}{current_user.id}"

    hash = pbkdf2(plaintext, salt, iterations, digest=digest)

    return hash


def b64enc(b: bytes) -> str:
    return base64.standard_b64encode(b).decode("utf8")


def pg_scram_sha256(passwd: str) -> str:
    digest_len = 32
    iterations = 4096
    salt = os.urandom(16)
    digest_key = hashlib.pbkdf2_hmac(
        "sha256", passwd.encode("utf8"), salt, iterations, digest_len
    )
    client_key = hmac.digest(digest_key, "Client Key".encode("utf8"), "sha256")
    stored_key = hashlib.sha256(client_key).digest()
    server_key = hmac.digest(digest_key, "Server Key".encode("utf8"), "sha256")
    return (
        f"SCRAM-SHA-256${iterations}:{b64enc(salt)}"
        f"${b64enc(stored_key)}:{b64enc(server_key)}"
    )
