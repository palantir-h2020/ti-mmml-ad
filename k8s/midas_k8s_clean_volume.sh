#!/usr/bin/env bash

BASEPATH="/media/palantir-nfs/NEC"
mkdir -p ${BASEPATH}/volumes/midas
ls -l ${BASEPATH}/volumes/midas
echo
rm -rf ${BASEPATH}/volumes/midas/*
chmod -R 777 ${BASEPATH}
echo
ls -l ${BASEPATH}/volumes/midas
