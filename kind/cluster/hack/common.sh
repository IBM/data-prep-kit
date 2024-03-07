#!/usr/bin/env bash

set -o errexit
set -o nounset
set -o pipefail

source ${ROOT_DIR}/requirements.env

os="unknown"

if [[ "$OSTYPE" == "linux-gnu" ]]; then
	os="linux"
elif [[ "$OSTYPE" == "darwin"* ]]; then
	os="darwin"
fi

if [[ "$os" == "unknown" ]]; then
	echo "OS '$OSTYPE' not supported. Aborting." >&2
	exit 1
fi

if [ ! -f ${KIND_DIR}/hack/.helper-functions.sh ]; then
	echo "Download helper-functions.sh"
	TEKTON_KFP_SERVER_VERSION=1.8.1
	wget https://raw.githubusercontent.com/kubeflow/kfp-tekton/v${TEKTON_KFP_SERVER_VERSION}/scripts/deploy/iks/helper-functions.sh -O ${KIND_DIR}/hack/.helper-functions.sh
fi
source ${KIND_DIR}/hack/.helper-functions.sh


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
	echo "$header$*$reset"
}
