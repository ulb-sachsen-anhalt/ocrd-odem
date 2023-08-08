#!/bin/bash

#set -e

# check and enforce sude
if [[ $UID != 0 ]]; then
    echo "Please run this script with sudo:"
    echo "sudo $0 $*"
    exit 1
fi

# ocr functional account properties
OCR_NAME=ocr
OCR_GROUP=ocr
OCR_GID=40367
OCR_UID=567
OCR_PSW=$1

echo "create OCR FA account ${OCR_UID}:${OCR_GID}"
addgroup ${OCR_GROUP} --gid ${OCR_GID}
useradd ${OCR_NAME} --uid ${OCR_UID} --gid ${OCR_GID} -p "${OCR_PSW}"
mkdir /home/${OCR_NAME}
cp -rT /etc/skel /home/${OCR_NAME} # Basis-Provisionierung
chown -R ${OCR_NAME}:${OCR_NAME} /home/${OCR_NAME}

echo "setup docker and gitlab-runner respositories"
# https://docs.docker.com/engine/install/ubuntu/
# https://docs.gitlab.com/runner/install/linux-repository.html
apt-get update
apt-get install -y ca-certificates curl gnupg lsb-release
mkdir -m 0755 -p /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --always-trust --dearmor -o /etc/apt/keyrings/docker.gpg
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
curl -L "https://packages.gitlab.com/install/repositories/runner/gitlab-runner/script.deb.sh" | sudo bash

apt-get update
apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin gitlab-runner \
  maven openjdk-11-jdk zip python3-venv

echo "prepare docker"
docker pull ocrd/all:2022-08-15

echo "add groups to accounts"
usermod -aG ${OCR_GROUP} gitlab-runner
usermod -aG gitlab-runner ${OCR_NAME}
usermod -aG docker gitlab-runner
usermod -aG docker ${OCR_NAME}

echo "set deafult shell for user ${OCR_NAME}"
sudo usermod --shell /bin/bash ${OCR_NAME}


echo "create directories for user ${OCR_NAME}"
mkdir /home/${OCR_NAME}/ulb-ocr-odem
mkdir /home/${OCR_NAME}/odem-wrk-dir
mkdir /home/${OCR_NAME}/odem-export
mkdir /home/${OCR_NAME}/odem-tessdata
mkdir /home/${OCR_NAME}/odem-tmp-export
mkdir /home/${OCR_NAME}/odem-log
chown -R ${OCR_NAME}:${OCR_NAME} /home/${OCR_NAME}
chmod g+w -R /home/${OCR_NAME}/

echo "setup for worker $(hostname) done"
