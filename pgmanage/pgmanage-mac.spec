# -*- mode: python ; coding: utf-8 -*-
import os

# since pyinstaller does not have option to specify folders to exclude
# we use basic filtering
# https://github.com/orgs/pyinstaller/discussions/6126

exclude_patterns = [
  os.path.join('js', 'pgmanage_frontend'),
]

block_cipher = None

data_files_server = [
  ('pgmanage.db','.'),
  ('config.py','.'),
  ('app/static','app/static'),
  ('app/include','app/include'),
  ('app/templates','app/templates'),
  ('app/plugins','app/plugins'),
]


a = Analysis(['pgmanage-server.py'],
             pathex=[],
             binaries=[],
             datas=data_files_server,
             hiddenimports=['cheroot.ssl','cheroot.ssl.builtin','psycopg2','paramiko', 'pkg_resources.extern'],
             hookspath=[],
             runtime_hooks=[],
             excludes=[],
             win_no_prefer_redirects=False,
             win_private_assemblies=False,
             cipher=block_cipher,
             noarchive=False)
a.datas = [entry for entry in a.datas if not any(pattern in entry[0] for pattern in exclude_patterns)]

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
          console=True,
          disable_windowed_traceback=False,
          argv_emulation=False,
          target_arch=None,
          codesign_identity=None,
          entitlements_file=None,
          )

coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='pgmanage-server',
)