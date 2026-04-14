#!/bin/bash

set -o xtrace
set -o errexit  # Exit the script with error if any of the commands fail

# Supported/used environment variables:
#  JAVA_VERSION                       Should be JDK17
#  AWS_CREDENTIAL_PROVIDER           "builtIn", 'awsSdkV1', 'awsSdkV2'
############################################
#            Main Program                  #
############################################

if ! which java ; then
    echo "Installing java..."
    apt update
    apt install openjdk-17-jdk -y

    export JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64"
    export JAVA_VERSION=17
fi

if ! which git ; then
    echo "installing git..."
    apt update
    apt install git -y
fi

echo "Checking gradle"
export GRADLE_RO_DEP_CACHE="/root/src/build/gradle-cache"
cd /root/src
./gradlew -version

echo "Building buildSrc classes"
./gradlew :buildSrc:classes --build-cache

echo "Building core classes"
./gradlew driver-core:classes --build-cache

echo "Running tests with Java ${JAVA_VERSION}"

./gradlew -PjavaVersion="${JAVA_VERSION}" -Dorg.mongodb.test.uri="${MONGODB_URI}" \
-Dorg.mongodb.test.aws.credential.provider="${AWS_CREDENTIAL_PROVIDER}" \
--stacktrace --info --build-cache driver-core:test --tests AwsAuthenticationSpecification
