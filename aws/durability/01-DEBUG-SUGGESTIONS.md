When debugging container or server issues on your remote EC2 instance, you have a few quick commands you can run directly from your laptop via SSH.

Here is how to inspect logs, check for crashes, and verify disk mounts if something goes wrong.

---

### 1. Tail Live Container Logs (The Most Useful)

Since Axon Server (and most database Docker containers) log directly to `stdout`/`stderr`, `docker logs` is your primary window into what is happening.

To live-stream (tail) the container logs from your laptop:

**For Axon Server:**

```bash
ssh -i esb-durability-aws-ssh-key.pem ubuntu@<INSTANCE_IP> "sudo docker logs -f my-axon-server-dcb"

```

**For UmaDB (or any other container):**

```bash
# First check the container name/ID with 'sudo docker ps'
ssh -i esb-durability-aws-ssh-key.pem ubuntu@<INSTANCE_IP> "sudo docker logs -f <CONTAINER_NAME_OR_ID>"

```

---

### 2. Check if the Container Crashed or Exited

If the container boots and immediately stops (e.g., due to an out-of-memory error, invalid configuration flag, or corrupted log segment during recovery), `docker ps` won't show it.

Run this to see all containers including stopped ones and their exit codes:

```bash
ssh -i esb-durability-aws-ssh-key.pem ubuntu@<INSTANCE_IP> "sudo docker ps -a"

```

If you see `Exited (1)` or `Exited (137)` (137 usually means killed by Out-Of-Memory killer), print the last 100 lines of logs prior to the crash:

```bash
ssh -i esb-durability-aws-ssh-key.pem ubuntu@<INSTANCE_IP> "sudo docker logs --tail 100 my-axon-server-dcb"

```

---

### 3. Debugging Disk & Mount Issues

If Axon Server throws a `java.io.IOException` or permission error:

1. **Verify the EBS volume is actually mounted:**
```bash
ssh -i esb-durability-aws-ssh-key.pem ubuntu@<INSTANCE_IP> "df -h /mnt/data"

```


*You should see `/dev/nvme1n1` mounted under `/mnt/data` with available space.*
2. **Check directory permissions on the host:**
```bash
ssh -i esb-durability-aws-ssh-key.pem ubuntu@<INSTANCE_IP> "ls -la /mnt/data"

```


*If you see permission issues, you can temporarily grant full access via `sudo chmod -R 777 /mnt/data`.*

---

### 4. Check Kernel & Docker System Errors

If the instance itself feels unresponsive or Docker is failing:

* **View system kernel logs (EBS connection drops, OOM killer messages):**
```bash
ssh -i esb-durability-aws-ssh-key.pem ubuntu@<INSTANCE_IP> "sudo dmesg -T | tail -n 50"

```


* **View Docker daemon logs:**
```bash
ssh -i esb-durability-aws-ssh-key.pem ubuntu@<INSTANCE_IP> "sudo journalctl -u docker --no-pager -n 50"

```



---

### Quick One-Liner Script Hack for your Setup

If you want to quickly SSH into whatever node is currently running without looking up the IP in AWS, you can use `.test-state` like this:

```bash
# Quick SSH into whichever instance is active
IP=$(aws ec2 describe-instances --instance-ids $(tail -n 1 .test-state | cut -d'=' -f2) --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
ssh -i esb-durability-aws-ssh-key.pem ubuntu@$IP

```