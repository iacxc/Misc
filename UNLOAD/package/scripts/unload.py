
import os
import subprocess
import sys

from resource_management import *
from resource_management.core.logger import Logger
from resource_management.core.resources.system import Directory


app_name = 'unloadCron'
app_exec = app_name + '.pl'
app_pid = app_name + '.pid'

def read_pid(pidfile):
    try:
        with file(pidfile) as f:
            pid = f.read().strip()

        return int(pid)
    except IOError:  # file not exist
        return 0


def kill_process(pid):
    import signal
    try:
        Logger.info('Killing process %d' % pid)
        os.kill(pid, signal.SIGTERM)
    except OSError:
        pass


class Slave(Script):
    def install(self, env):
        self.install_packages(env)

    def configure(self, env):
        import params
       
        Logger.info('Configuring the service')

        config = self.get_config()
        Directory('%s/HDFS' % params.base_dir, create_parents=True)
        Directory('%s/HDFS/HDFScontrol' % params.base_dir)
        Directory('%s/HDFS/HDFSlogs' % params.base_dir)
        Directory('%s/HDFS/HDFSstaging' % params.base_dir)
        Directory('%s/HDFS/HDFSprocessing' % params.base_dir)
        Directory('%s/HDFS/HDFSqueue' % params.base_dir)

        File('%s/%s' % (params.base_dir, app_exec),
             mode=0755,
             content=StaticFile(app_exec))

        Logger.info('Finished configuring the slave')

    def start(self, env):
        import params
        env.set_params(params)
        self.configure(env)

        pid_file = '%s/%s' % (params.base_dir, app_pid)
        pid = read_pid(pid_file)
        if pid != 0:
            kill_process(pid)
            File(pid_file, action='delete')

        cmd = ['/usr/bin/perl', 
               '%s/%s' % (params.base_dir, app_exec),
               params.base_dir,
               params.concurrency,
               params.throttle,
               params.garbage_seconds]
        p = subprocess.Popen(cmd)

        with file(pid_file, 'w') as f:
            f.write('%d\n' % p.pid)

        Logger.info('Started unload service')

    def stop(self, env):
        import params
        env.set_params(params)

        pid_file = '%s/%s' % (params.base_dir, app_pid)
        pid = read_pid(pid_file)
        kill_process(pid)
        File(pid_file, action='delete')

        Logger.info('Service stopped')

    def status(self, env):
        import status_params as params
        env.set_params(params)
        Logger.info('Status ...')

        pid_file = '%s/%s' % (params.base_dir, app_pid)
        Logger.info('Checking pid file %s' % pid_file)

        check_process_status(pid_file)
 
    def install_package(self, env):
        print 'Installing slave', env


if __name__ == "__main__":
    Slave().execute()
