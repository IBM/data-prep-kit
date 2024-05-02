#!/usr/bin/env bash

set -o errexit
set -o nounset
set -o pipefail

current_os="unknown"

case "$OSTYPE" in
  linux-gnu)
    current_os="linux"
    ;;
  darwin*)
    current_os="darwin"
    ;;
esac

if [[ "$current_os" == "unknown" ]]; then
  printf "OS '%s' not supported. Aborting.\n" "$OSTYPE" >&2
  exit 1
fi

if [ ! -f "${ROOT_DIR}/hack/.helper-functions.sh" ]; then
  printf "Download helper-functions.sh\n"
  if command -v wget &>/dev/null; then
    TEKTON_KFP_SERVER_VERSION=1.8.1
    wget "https://raw.githubusercontent.com/kubeflow/kfp-tekton/v${TEKTON_KFP_SERVER_VERSION}/scripts/deploy/iks/helper-functions.sh" -O "${ROOT_DIR}/hack/.helper-functions.sh"
  else
    printf "Error: wget not found. Aborting.\n" >&2
    exit 1
  fi
fi

source "${ROOT_DIR}/hack/.helper-functions.sh"

# Turn colors in this script off by setting the NO_COLOR variable in your
# environment to any value:
#
# $ NO_COLOR=1 test.sh
NO_COLOR=${NO_COLOR:-""}
if [ -z "$NO_COLOR" ]; then
  header=$'\e[1;33m'
  reset=$'\e[0m'
else
  header=''
  reset=''
fi

function header_text {
  printf "%s%s%s\n" "$header" "$*" "$reset"
}