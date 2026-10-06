import paramiko
import sys
import time

sys.stdout.reconfigure(encoding='utf-8')

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect('157.15.124.192', username='root', password='Bara0113!')

print("1. Uploading modified files...")
sftp = ssh.open_sftp()
sftp.put(r'c:\projects\home-automation\backend\main.go', '/opt/bara-sense/backend/main.go')
sftp.put(r'c:\projects\home-automation\backend\tuya_history.go', '/opt/bara-sense/backend/tuya_history.go')
sftp.put(r'c:\projects\home-automation\backend\infrastructure\tuya\cloud_client.go', '/opt/bara-sense/backend/infrastructure/tuya/cloud_client.go')
sftp.put(r'c:\projects\home-automation\backend\.env', '/opt/bara-sense/.env')
sftp.put(r'c:\projects\home-automation\backend\.env', '/opt/bara-sense/backend/.env')
sftp.close()
print("   Uploaded successfully.")

def run_cmd(cmd):
    print("Executing:", cmd)
    stdin, stdout, stderr = ssh.exec_command(cmd)
    while not stdout.channel.exit_status_ready():
        time.sleep(1)
    out = stdout.read().decode('utf-8', errors='ignore')
    err = stderr.read().decode('utf-8', errors='ignore')
    if out:
        print("OUT:", out[-500:])
    if err:
        print("ERR:", err[-500:])

print("2. Building podman container...")
run_cmd("cd /opt/bara-sense && podman build -t localhost/bara-sense-app:latest .")

print("3. Removing old container...")
run_cmd("podman rm -f bara-sense-app")

print("4. Starting new container with env file...")
run_cmd("podman run -d --name bara-sense-app --network bara-network -p 3000:3000 --env-file /opt/bara-sense/.env -e POSTGRES_URL='postgres://postgres:postgres@bara-sense-db:5432/home_automation?sslmode=disable' localhost/bara-sense-app:latest")

time.sleep(2)
run_cmd("podman ps")

ssh.close()
