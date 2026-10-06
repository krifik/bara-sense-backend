import paramiko
import sys

sys.stdout.reconfigure(encoding='utf-8')
ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect('157.15.124.192', username='root', password='Bara0113!')

_, stdout, _ = ssh.exec_command('podman logs bara-sense-app 2>&1')
lines = stdout.readlines()
print(f"Total lines in log: {len(lines)}")
print("--- LAST 40 LINES ---")
for l in lines[-40:]:
    sys.stdout.write(l)

ssh.close()
