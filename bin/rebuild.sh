#!/bin/sh

DIR=./
mkdir -p ${DIR}/recipes/
mkdir -p ${DIR}/index/
mkdir -p ${DIR}/containers/

rm -f ${DIR}/recipes/*
rm -f ${DIR}/index/*
rm -f ${DIR}/containers/container.pool
rm -f ${DIR}/destor.stat
rm -f ${DIR}/manifest
rm -rf ${DIR}/restore/*
