import paramiko

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect('157.15.124.192', username='root', password='Bara0113!')

sftp = ssh.open_sftp()
sftp.put(r'c:\projects\home-automation\backend\main.go', '/opt/bara-sense/backend/main.go')
sftp.put(r'c:\projects\home-automation\backend\tuya_history.go', '/opt/bara-sense/backend/tuya_history.go')
sftp.put(r'c:\projects\home-automation\backend\infrastructure\tuya\cloud_client.go', '/opt/bara-sense/backend/infrastructure/tuya/cloud_client.go')
sftp.put(r'c:\projects\home-automation\backend\.env', '/opt/bara-sense/.env')
sftp.put(r'c:\projects\home-automation\backend\.env', '/opt/bara-sense/backend/.env')
sftp.close()

cmds = [
    "cd /opt/bara-sense && podman build -t localhost/bara-sense-app:latest .",
    "podman rm -f bara-sense-app",
    "podman run -d --name bara-sense-app --network bara-network -p 3000:3000 --env-file /opt/bara-sense/.env -e POSTGRES_URL='postgres://postgres:postgres@bara-sense-db:5432/home_automation?sslmode=disable' localhost/bara-sense-app:latest",
    "podman ps"
]

full_cmd = " && ".join(cmds)
stdin, stdout, stderr = ssh.exec_command(full_cmd)
print("STDOUT:")
print(stdout.read().decode('utf-8', errors='ignore'))
print("STDERR:")
print(stderr.read().decode('utf-8', errors='ignore'))
ssh.close()
