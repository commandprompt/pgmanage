# -*- mode: python -*-

import os

# since pyinstaller does not have option to specify folders to exclude
# we use basic filtering
# https://github.com/orgs/pyinstaller/discussions/6126

exclude_patterns = [
  os.path.join('js', 'pgmanage_frontend'),
]

block_cipher = None

data_files = [
  ('pgmanage.db','.'),
  ('config.py','.'),
  ('app/static','app/static'),
  ('app/include','app/include'),
  ('app/templates','app/templates'),
  ('app/plugins','app/plugins'),
]

a = Analysis(['pgmanage-server.py'],
             pathex=['app/include/'],
             binaries=[],
             datas=data_files,
             hiddenimports=['cheroot.ssl','cheroot.ssl.builtin','psycopg2','paramiko', 'pkg_resources.extern', 'cryptography.hazmat.primitives.kdf.pbkdf2'],
             hookspath=[],
             runtime_hooks=[],
             excludes=[],
             win_no_prefer_redirects=False,
             win_private_assemblies=False,
             cipher=block_cipher)

a.datas = [entry for entry in a.datas if not any(pattern in entry[0] for pattern in exclude_patterns)]

pyz = PYZ(a.pure, a.zipped_data,
            cipher=block_cipher)

exe = EXE(pyz,
         a.scripts,
         [],
         a.binaries,
         a.zipfiles,
         a.datas,
         name='pgmanage-server',
         debug=False,
         strip=False,
         upx=True,
         runtime_tmpdir=None,
         console=True )
