# README

This README reports the setup instructions for the demo of the MIDAS [1] Anomaly Detection module, based on the open-source implementation [2].

Kafka, MIDAS and the alerts visualizer are all provided as Docker containers.

A 5-minutes sample of CIC-IDS2017 dataset [3] is provided and can be pushed into Kafka from a Python script.

## Requirements (tested on Ubuntu 20.04.3 LTS)

### Docker

Install Docker 20.10 (https://docs.docker.com/engine/install/ubuntu/)
```
sudo apt update
sudo apt install -y ca-certificates curl gnupg lsb-release
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu \
  $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

sudo apt update
sudo apt install -y docker-ce docker-ce-cli containerd.io
sudo docker run hello-world

sudo curl -L "https://github.com/docker/compose/releases/download/1.29.2/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose
```
Configure Docker to be used as non-root user (https://docs.docker.com/engine/install/linux-postinstall/)
```
sudo groupadd docker
sudo usermod -aG docker $USER
sudo shutdown -r now
docker run hello-world
```

### Tmux
Install and configure `tmux`
```
sudo apt install -y tmux
echo "set -g default-terminal \"xterm-256color\"" >> ~/.tmux.conf
```

### kafka-python module (required only by non-Dockerised CIC-IDS2017 replay tool)
```
sudo apt install -y python3-pip
pip3 install kafka-python
```

### Kafka tools (OPTIONAL)
```
cd ~/
wget https://dlcdn.apache.org/kafka/3.0.0/kafka_2.13-3.0.0.tgz
tar -xzf kafka_2.13-3.0.0.tgz
sudo apt install -y default-jre
# Modify KAFKA_TOOLS_PATH in the following file with the Kafka tools installation folder
nano [REPO_ROOT_DIR]/common.sh
```

## Run demo
```
cd [REPO_ROOT_DIR]
./run_all_in_tmux_docker.sh
```
Select option 1 and press ENTER.

The download of all the Docker images can take several minutes.

Once all the terminals are ready (i.e. all the 3 terminals show `Waiting for new messages...`), press ENTER to replay CIC-IDS2017 dataset.

### References
```
[1] Siddharth Bhatia, Bryan Hooi, Minji Yoon, Kijung Shin, Christos Faloutsos - MIDAS: Microcluster-Based Detector of Anomalies in Edge Streams - AAAI 2020
[2] https://github.com/Stream-AD/MIDAS
[3] https://www.unb.ca/cic/datasets/ids-2017.html
```
