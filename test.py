import os
import subprocess


from config import local_env

for key, value in local_env.items():
    os.environ[key] = value
os.environ['PWD'] = os.getcwd()

# this will be used to flush buffers at some point
subprocess.run(
    ['setx', 'PWD', os.getcwd(), '&', 'docker', 'restart', 'adb_2019_db_1'],
    shell=True,
    env=os.environ.copy()
)
