import os
import base64
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives import hashes


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


def make_hash(plaintext):
    digest = hashes.Hash(hashes.SHA256())
    digest.update(plaintext.encode())
    return digest.finalize()