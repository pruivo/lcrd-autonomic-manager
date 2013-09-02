#!/bin/bash

DIR=$(dirname $0)

mvn install:install-file -Dfile=${DIR}/3rdPartyJars/dap-framework-1.0.jar -DgroupId=pt.ist.clustering -DartifactId=lda -Dversion=1.0 -Dpackaging=jar
