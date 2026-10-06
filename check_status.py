import paramiko
import sys
sys.stdout.reconfigure(encoding='utf-8')

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect('157.15.124.192', username='root', password='Bara0113!')

def run(cmd):
    print(">>>", cmd)
    stdin, stdout, stderr = ssh.exec_command(cmd)
    out = stdout.read().decode('utf-8', errors='ignore')
    err = stderr.read().decode('utf-8', errors='ignore')
    if out:
        print("OUT:", out.strip())
    if err:
        print("ERR:", err.strip())

run("podman ps -a")
run("podman logs --tail 20 bara-sense-app")

ssh.close()
