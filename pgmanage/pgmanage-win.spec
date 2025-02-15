# -*- mode: python ; coding: utf-8 -*-

import os

# since pyinstaller does not have option to specify folders to exclude
# we use basic filtering
# https://github.com/orgs/pyinstaller/discussions/6126

exclude_patterns = [
  os.path.join('static', 'pgmanage_frontend'),
  '.dist-info',
  '.py',
  'django\\contrib\\gis',
  'django\\contrib\\humanize',
  'django\\contrib\\flatpages',
  'django\\contrib\\sitemaps',
  'django\\contrib\\syndication',
  'django\\contrib\\admindocs',
]

block_cipher = None

data_files_server = [
  ('pgmanage.db','.'),
  ('config.py','.'),
  ('app/static','app/static'),
  ('app/include','app/include'),
  ('app/templates','app/templates'),
  ('app/plugins','app/plugins'),
  ('app/bgjob/process_executor.py', 'app/bgjob'),
]

a = Analysis(['pgmanage-server.py'],
             binaries=[],
             datas=data_files_server,
             hiddenimports=['cheroot.ssl','cheroot.ssl.builtin','psycopg2','paramiko', 'pkg_resources.extern', 'cryptography.hazmat.primitives.kdf.pbkdf2'],
             hookspath=[],
             runtime_hooks=[],
             excludes=['django.contrib.gis', 'django.contrib.sitemaps', 'django.contrib.flatpages', 'django.contrib.syndication', 'django.contrib.admindocs', 'django.contrib.humanize'],
             win_no_prefer_redirects=False,
             win_private_assemblies=False,
             cipher=block_cipher,
             noarchive=False)

             
# config.py gets removed by the next expression, keep it for restoring later
configpy = [entry for entry in a.datas if 'config.py' in entry[0]]
a.datas = [entry for entry in a.datas if not any(pattern in entry[0] for pattern in exclude_patterns)]
a.datas = a.datas + configpy

pyz = PYZ(a.pure, a.zipped_data,
             cipher=block_cipher)
exe = EXE(pyz,
          a.scripts,
          [],
          exclude_binaries=True,
          name='pgmanage-server',
          debug=False,
          bootloader_ignore_signals=False,
          strip=False,
          upx=True,
          console=True )
coll = COLLECT(exe,
               a.binaries,
               a.zipfiles,
               a.datas,
               strip=False,
               upx=True,
               upx_exclude=[],
               name='pgmanage-server')
