"""

"""

from ambari_commons import OSCheck
from resource_management.libraries.script import Script

config = Script.get_config()

base_dir = config['configurations']['unload-env']['base.dir']
concurrency = config['configurations']['unload-site']['concurrency']
throttle = config['configurations']['unload-site']['throttle']
garbage_seconds = config['configurations']['unload-site']['garbage.seconds']
