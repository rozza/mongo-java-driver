#!/bin/bash

set -o xtrace
set -o errexit  # Exit the script with error if any of the commands fail

# Supported/used environment variables:
#  DRIVERS_TOOLS                     The location of the driver tools
#  MONGODB_BINARIES                  The location of the mongodb binaries
#  ARCHIVE_FILE_PATH                 The location of the archived files
############################################
#            Main Program                  #
############################################
RELATIVE_DIR_PATH="$(dirname "${BASH_SOURCE:-$0}")"
. "${RELATIVE_DIR_PATH}/setup-env.bash"

echo "Running MONGODB-AWS ECS authentication tests"

# Set up the target directory.
AUTH_AWS_DIR=${DRIVERS_TOOLS}/.evergreen/auth_aws
ECS_SRC_DIR=$AUTH_AWS_DIR/src
mkdir -p $ECS_SRC_DIR/.evergreen

# Move the test script to the correct location.
cp ${PROJECT_DIRECTORY}/.evergreen/run-mongodb-aws-ecs-test.sh $ECS_SRC_DIR/.evergreen
cp ${PROJECT_DIRECTORY}/.evergreen/setup-env.bash $ECS_SRC_DIR/.evergreen

# Copy over cached and zipped files
tar -xvf $ARCHIVE_FILE_PATH -C $ECS_SRC_DIR

# Run the test
PROJECT_DIRECTORY="$ECS_SRC_DIR" GRADLE_RO_DEP_CACHE="/root/src/build/gradle-cache" MONGODB_BINARIES="$MONGODB_BINARIES" \
AWS_CREDENTIAL_PROVIDER="$AWS_CREDENTIAL_PROVIDER" $AUTH_AWS_DIR/aws_setup.sh ecs
