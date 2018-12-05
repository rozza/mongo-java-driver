#!/bin/bash

#set -o xtrace   # Write all commands first to stderr
#set -o errexit  # Exit the script with error if any of the commands fail

############################################
#            Main Program                  #
############################################

SDK_HOME=$PWD/.android
if [ ! -e  $SDK_HOME ]; then
    echo "Installing ANDROID SDK"
    mkdir -p $SDK_HOME
    (
        cd $SDK_HOME
        export JAVA_HOME="/opt/java/jdk8"

        export ANDROID_HOME=${SDK_HOME}
        export ANDROID_SDK_ROOT=${SDK_HOME}
        export ANDROID_SDK_HOME=${SDK_HOME}

        SDK_PACKAGE=sdk-tools-linux-4333796.zip
        curl -O https://dl.google.com/android/repository/$SDK_PACKAGE
        unzip $SDK_PACKAGE
        yes | $SDK_HOME/tools/bin/sdkmanager --channel=0 \
            "platforms;android-28"  \
            "platform-tools"  \
            "build-tools;28.0.2" \
            "emulator" \
            "system-images;android-21;google_apis;x86_64"

        PLATFORM_TOOLS=platform-tools-latest-linux.zip
        curl -O https://dl.google.com/android/repository/$PLATFORM_TOOLS
        unzip $PLATFORM_TOOLS
    )
fi

(
    export JAVA_HOME="/opt/java/jdk8"

    export ANDROID_HOME=${SDK_HOME}
    export ANDROID_SDK_ROOT=${SDK_HOME}
    export ANDROID_SDK_HOME=${SDK_HOME}

    echo no | $SDK_HOME/tools/bin/avdmanager create avd -n embeddedTest_x86_64 -c 1000M -k "system-images;android-21;google_apis;x86_64" -f
    $SDK_HOME/emulator/emulator64-x86 -avd embeddedTest_x86_64 -no-audio -no-window -no-snapshot -wipe-data &
    $SDK_HOME/platform-tools/adb wait-for-device

    # Belt and braces waiting for the device
    bootanim=""
    failcounter=0
    timeout_in_sec=360

    until [[ "$bootanim" =~ "stopped" ]]; do
      bootanim=`$SDK_HOME/platform-tools/adb -e shell getprop init.svc.bootanim 2>&1 &`
      if [[ "$bootanim" =~ "device not found" || "$bootanim" =~ "device offline"
        || "$bootanim" =~ "running" ]]; then
        let "failcounter += 1"
        echo "Waiting for emulator to start"
        if [[ $failcounter -gt timeout_in_sec ]]; then
          echo "Timeout ($timeout_in_sec seconds) reached; failed to start emulator"
          exit 1
        fi
      fi
      sleep 1
    done
    echo "Emulator is ready"
)

export JAVA_HOME="/opt/java/jdk9"
export ANDROID_HOME=${SDK_HOME}

function shutdownEmulators {
  $SDK_HOME/platform-tools/adb devices | grep emulator | cut -f1 | while read line; do $SDK_HOME/platform-tools/adb -s $line emu kill; done;
}

trap shutdownEmulators EXIT

echo "Running android tests"
./gradlew -version
./gradlew --stacktrace --info :driver-embedded-android:connectedAndroidTest -DANDROID_HOME=$SDK_HOME

