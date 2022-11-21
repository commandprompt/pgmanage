import os
import base64
import hashlib

from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from django.utils.crypto import pbkdf2


def encrypt(plaintext, key):
    iv = os.urandom(16)
    cipher = Cipher(algorithms.AES(key), modes.CFB(iv))
    encryptor = cipher.encryptor()
    ct = encryptor.update(plaintext.encode()) + encryptor.finalize()
    return base64.b64encode(iv+ct).decode()


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
    salt = f'{current_user.date_joined}{current_user.id}'

    hash = pbkdf2(plaintext, salt, iterations, digest=digest)

    return hash
