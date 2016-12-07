"""

"""

from ambari_commons import OSCheck
from resource_management.libraries.script import Script
from resource_management.libraries.functions import format

config = Script.get_config()

name = 'unloadCron'
base_dir = config['configurations']['unload-env']['base.dir']
user = config['configurations']['unload-env']['unload_user']
pid_file = format("{base_dir}/{user}-{name}.pid")
