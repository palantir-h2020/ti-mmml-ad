set -o xtrace

docker ps -a
docker stop $(docker ps -a |  awk 'NR>1 { print $1}')
docker rm $(docker ps -a | awk 'NR>1 { print $1}')
docker volume rm $(docker volume ls -q)
docker ps -a
