"""

"""

from ambari_commons import OSCheck
from resource_management.libraries.script import Script

config = Script.get_config()

base_dir = config['configurations']['unload-env']['base.dir']
concurrency = config['configurations']['unload-env']['concurrency']
throttle = config['configurations']['unload-env']['throttle']
garbage_seconds = config['configurations']['unload-env']['garbage.seconds']
