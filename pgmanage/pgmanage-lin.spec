# -*- mode: python -*-

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
             hiddenimports=['cheroot.ssl','cheroot.ssl.builtin','psycopg2','paramiko'],
             hookspath=[],
             runtime_hooks=[],
             excludes=[],
             win_no_prefer_redirects=False,
             win_private_assemblies=False,
             cipher=block_cipher)

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
