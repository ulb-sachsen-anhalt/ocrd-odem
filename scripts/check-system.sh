#!/bin/bash

# script to check if runtime
# requirements for workers met

declare -A errors

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

if ! [ "$(which zip)" ]
then
  echo zip not installed
  errors["zip"]="missing"
fi 

if ! [ "$(which  mvn)" ]
then
  errors["maven"]="maven missing ! "
fi 

if ! [ "$(which  java)" ]
then
  errors["java"]="java missing ! "
fi 

# only for virtual machines relevant
# double-brackets required for ignore case matches
if [[ "${PLATTFORM}" == "vm" ]]; then
  if ! [ -d "/data/ocr" ]
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

