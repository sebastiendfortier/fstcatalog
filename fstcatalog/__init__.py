from .fstcatalog import *
p = Path(os.path.abspath(__file__))
v_file = open(p.parent / 'VERSION')
__version__ = v_file.readline().strip()
v_file.close()

