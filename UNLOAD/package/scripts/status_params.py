"""

"""

from ambari_commons import OSCheck
from resource_management.libraries.script import Script

config = Script.get_config()

base_dir = config['configurations']['unload-env']['base.dir']
