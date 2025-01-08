#!/bin/bash

# script to check if runtime
# requirements for workers met

declare -A errors

DATA_SHARE_MOUNT_POINT=/data/ocr

PLATTFORM=${1:-ironman}

ROLE=${2:-worker}

echo "[INFO ] checking plattform '${PLATTFORM}' for role '${ROLE}'"

# set ignore cases for string substitutions
shopt -s nocasematch

# availability of base image only for worker machines relevant
# double-brackets required for ignore case matches
if [[ "${ROLE}" == "worker" ]]; then
  if ! [ "$(which docker)" ]; then
    echo "[ERROR] docker not installed on ${ROLE}"
    errors["docker"]="missing"
  fi 
  if [ ! "$(docker image ls -f=reference="ocrd/all:*" -q)" ]; then
    errors["docker"]="docker ocrd/all image missing ! "
  fi 
fi

THE_ZIP=zip
if ! [ "$(which ${THE_ZIP})" ]
then
  errors["${THE_ZIP}"]="missing ${THE_ZIP}"
fi 

THE_PYTHON=python3.10
if ! [ "$(which ${THE_PYTHON})" ]
then
  errors["${THE_PYTHON}"]="missing ${THE_PYTHON}"
fi 

# only for virtual machines relevant
# double-brackets required for ignore case matches
if [[ "${PLATTFORM}" == "vm" ]]; then
  if ! [ -d "${DATA_SHARE_MOUNT_POINT}" ]
  then
    errors["data mount"]="data share not mounted ! "
  else
    echo "[INFO ] data-share mounted"
  fi
else
  echo "[INFO ] no VM environment but '${PLATTFORM}', don't check data-mount"
fi

if [ -z "${errors[*]}" ]
then
  echo "[INFO ] requirements met for '${PLATTFORM}' / '${ROLE}'"
else
  echo "[ERROR] ${errors[*]}"
  exit 1
fi

