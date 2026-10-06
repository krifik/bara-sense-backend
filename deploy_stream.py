import paramiko
import sys
sys.stdout.reconfigure(encoding='utf-8')

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect('157.15.124.192', username='root', password='Bara0113!')

print("1. Uploading files...")
sftp = ssh.open_sftp()
sftp.put(r'c:\projects\home-automation\backend\main.go', '/opt/bara-sense/backend/main.go')
sftp.put(r'c:\projects\home-automation\backend\tuya_history.go', '/opt/bara-sense/backend/tuya_history.go')
sftp.put(r'c:\projects\home-automation\backend\infrastructure\tuya\cloud_client.go', '/opt/bara-sense/backend/infrastructure/tuya/cloud_client.go')
sftp.put(r'c:\projects\home-automation\backend\.env', '/opt/bara-sense/.env')
sftp.put(r'c:\projects\home-automation\backend\.env', '/opt/bara-sense/backend/.env')
sftp.close()
print("   Uploaded.")

print("2. Recompiling backend binary on host/container...")
stdin, stdout, stderr = ssh.exec_command("podman build -t localhost/bara-sense-app:latest /opt/bara-sense")
for line in stdout:
    print(line.strip())
print("STDERR:", stderr.read().decode('utf-8', errors='ignore'))

print("3. Replacing container...")
stdin, stdout, stderr = ssh.exec_command("podman rm -f bara-sense-app && podman run -d --name bara-sense-app --restart always --network bara-network -p 3000:3000 --env-file /opt/bara-sense/.env -e POSTGRES_URL='postgres://postgres:postgres@bara-sense-db:5432/home_automation?sslmode=disable' localhost/bara-sense-app:latest")
print("Run stdout:", stdout.read().decode())
print("Run stderr:", stderr.read().decode())

stdin, stdout, stderr = ssh.exec_command("podman ps")
print(stdout.read().decode())

ssh.close()
