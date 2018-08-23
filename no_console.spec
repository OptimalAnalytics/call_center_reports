# -*- mode: python -*-

import gooey
gooey_root = os.path.dirname(gooey.__file__)
gooey_languages = Tree(os.path.join(
    gooey_root, 'languages'), prefix='gooey/languages')
gooey_images = Tree('images', prefix='gooey/images')

block_cipher = None


a = Analysis(['process_reports.py'],
             pathex=['C:\\Users\\david\\Documents\\GitHub\\katie_reports'],
             binaries=[],
             datas=[],
             hiddenimports=['pandas._libs.tslibs.timedeltas',
                            'pandas._libs.tslibs.np_datetime',
                            'pandas._libs.tslibs.nattype',
                            'pandas._libs.skiplist'],
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
          a.binaries,
          a.zipfiles,
          a.datas,
          gooey_languages,
          gooey_images,
          name='RPC Processing',
          debug=False,
          strip=False,
          upx=True,
          runtime_tmpdir=None,
          console=False,
          icon=os.path.join('images', 'program_icon.ico'))
