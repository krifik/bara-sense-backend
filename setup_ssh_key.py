import paramiko
import sys

sys.stdout.reconfigure(encoding='utf-8')
ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect('157.15.124.192', username='root', password='Bara0113!')

script = """
if not test -f /root/.ssh/id_ed25519
    ssh-keygen -t ed25519 -N "" -f /root/.ssh/id_ed25519
end
cat /root/.ssh/id_ed25519.pub >> /root/.ssh/authorized_keys
chmod 600 /root/.ssh/authorized_keys
cat /root/.ssh/id_ed25519
"""

stdin, stdout, stderr = ssh.exec_command(script)
print("PRIVATE_KEY:")
print(stdout.read().decode('utf-8', errors='ignore'))
print("STDERR:", stderr.read().decode('utf-8', errors='ignore'))
ssh.close()
