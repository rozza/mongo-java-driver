# Java configurations for evergreen

# find_jdk <suffix>
# Searches platform-specific paths for a JDK directory matching the given suffix
# (e.g. "jdk17", "jdk21-graalce"). Sets REPLY to the found path, or empty string.
find_jdk() {
  local suffix="$1"
  local candidates=()

  case "$(uname -s)" in
    CYGWIN*|MINGW*|MSYS*)
      candidates=(
        "/cygdrive/c/java/${suffix}"
        "C:/java/${suffix}"
      )
      ;;
    Darwin)
      candidates=(
        "/opt/java/${suffix}"
        "/Library/Java/JavaVirtualMachines/${suffix}"
      )
      ;;
    *)
      candidates=(
        "/opt/java/${suffix}"
      )
      ;;
  esac

  REPLY=""
  for path in "${candidates[@]}"; do
    if [ -d "$path" ]; then
      # Gradle on macOS unconditionally uses Contents/Home if that directory exists,
      # even when it has no bin/java inside. If the JDK has a broken Contents/Home,
      # create a local wrapper with a correct Contents/Home symlink.
      if [ "$(uname -s)" = "Darwin" ] && [ -x "$path/bin/java" ] && [ ! -x "$path/Contents/Home/bin/java" ]; then
        local wrapper="${LOCAL_JDKS}/$(basename "$path")"
        mkdir -p "$wrapper/Contents"
        ln -sfn "$path" "$wrapper/Contents/Home"
        ln -sfn "$path/bin" "$wrapper/bin"
        ln -sfn "$path/lib" "$wrapper/lib"
        REPLY="$wrapper"
      else
        REPLY="$path"
      fi
      return
    fi
  done
}

LOCAL_JDKS="$(pwd)/.jdks"
mkdir -p "$LOCAL_JDKS"

find_jdk "jdk8";          [ -n "$REPLY" ] && export JDK8="$REPLY"
find_jdk "jdk11";         [ -n "$REPLY" ] && export JDK11="$REPLY"
find_jdk "jdk17";         [ -n "$REPLY" ] && export JDK17="$REPLY"
find_jdk "jdk21";         [ -n "$REPLY" ] && export JDK21="$REPLY"
# note that `JDK21_GRAALVM` is used in `run-graalvm-native-image-app.sh`
# by dynamically constructing the variable name
find_jdk "jdk21-graalce"; [ -n "$REPLY" ] && export JDK21_GRAALVM="$REPLY"

if [ -z "$JDK17" ]; then
  echo "ERROR: JDK17 not found. Aborting." >&2
  return 1 2>/dev/null || exit 1
fi

export JAVA_HOME=$JDK17

export JAVA_VERSION=${JAVA_VERSION:-17}

echo "Java Configs:"
echo "JAVA_HOME: ${JAVA_HOME}"
echo "Java test version: ${JAVA_VERSION}"
echo "Found JDKs:"
[ -n "$JDK8" ]          && echo "  JDK8:          $JDK8"
[ -n "$JDK11" ]         && echo "  JDK11:         $JDK11"
[ -n "$JDK17" ]         && echo "  JDK17:         $JDK17"
[ -n "$JDK21" ]         && echo "  JDK21:         $JDK21"
[ -n "$JDK21_GRAALVM" ] && echo "  JDK21_GRAALVM: $JDK21_GRAALVM"

# Rename environment variables for AWS, Azure, and GCP
if [ -f secrets-export.sh ]; then
  echo "Renaming secrets env variables"
  . secrets-export.sh

  export AWS_ACCESS_KEY_ID=$FLE_AWS_ACCESS_KEY_ID
  export AWS_SECRET_ACCESS_KEY=$FLE_AWS_SECRET_ACCESS_KEY
  export AWS_DEFAULT_REGION=$FLE_AWS_DEFAULT_REGION

  export AWS_ACCESS_KEY_ID_AWS_KMS_NAMED=$FLE_AWS_KEY2
  export AWS_SECRET_ACCESS_KEY_AWS_KMS_NAMED=$FLE_AWS_SECRET2

  export AWS_TEMP_ACCESS_KEY_ID=$CSFLE_AWS_TEMP_ACCESS_KEY_ID
  export AWS_TEMP_SECRET_ACCESS_KEY=$CSFLE_AWS_TEMP_SECRET_ACCESS_KEY
  export AWS_TEMP_SESSION_TOKEN=$CSFLE_AWS_TEMP_SESSION_TOKEN

  export AZURE_CLIENT_ID=$FLE_AZURE_CLIENTID
  export AZURE_TENANT_ID=$FLE_AZURE_TENANTID
  export AZURE_CLIENT_SECRET=$FLE_AZURE_CLIENTSECRET

  export GCP_EMAIL=$FLE_GCP_EMAIL
  export GCP_PRIVATE_KEY=$FLE_GCP_PRIVATEKEY

  # Unset AWS_SESSION_TOKEN if it is empty
  if [ -z "$AWS_SESSION_TOKEN" ];then
    unset AWS_SESSION_TOKEN
  fi

else
  echo "No secrets env variables found to rename"
fi
