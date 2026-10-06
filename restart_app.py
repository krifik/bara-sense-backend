import paramiko
import sys
sys.stdout.reconfigure(encoding='utf-8')

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect('157.15.124.192', username='root', password='Bara0113!')

print("1. Uploading .env...")
sftp = ssh.open_sftp()
sftp.put(r'c:\projects\home-automation\backend\.env', '/opt/bara-sense/.env')
sftp.put(r'c:\projects\home-automation\backend\.env', '/opt/bara-sense/backend/.env')
sftp.close()

print("2. Restarting container with new env...")
cmd = (
    "podman rm -f bara-sense-app && "
    "podman run -d --name bara-sense-app --restart always --network bara-network "
    "-p 3000:3000 --env-file /opt/bara-sense/.env "
    "-e POSTGRES_URL='postgres://postgres:postgres@bara-sense-db:5432/home_automation?sslmode=disable' "
    "localhost/bara-sense-app:latest"
)
stdin, stdout, stderr = ssh.exec_command(cmd)
print("STDOUT:", stdout.read().decode())
print("STDERR:", stderr.read().decode())

stdin, stdout, stderr = ssh.exec_command("podman ps")
print(stdout.read().decode())

ssh.close()
