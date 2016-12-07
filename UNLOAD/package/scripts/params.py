"""

"""

from resource_management.libraries.script import Script
from resource_management.libraries.functions import format


app = 'unloadCron'
app_exec = format('{app}.pl')

app_root = '/var/lib/unload'
app_sbin = format('{app_root}/sbin')
app_bin  = format('{app_root}/bin')
app_start = 'unload_start.sh'

config = Script.get_config()

base_dir = config['configurations']['unload-env']['base.dir']
user = config['configurations']['unload-env']['unload_user']
group = config['configurations']['unload-env']['user_group']

concurrency = config['configurations']['unload-site']['concurrency']
throttle = config['configurations']['unload-site']['throttle']
garbage_seconds = config['configurations']['unload-site']['garbage.seconds']
