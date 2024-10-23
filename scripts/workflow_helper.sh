#!/bin/bash


# This ensures that the script exits immediately if any command
# returns a non-zero status, preventing cases where the GitHub
# Action running the script might overlook an error if it occurs.
set -euo pipefail

op=$1

install_tools(){
        if [ -z "$KIND_VERSION" ] || [ -z "$HELM_VERSION" ] || [ -z "$KUBECTL_VERSION" ]; then
            echo "Missing tools versions"
            exit 1
        fi
        echo "Installing tools"

        curl -Lo /tmp/kind https://kind.sigs.k8s.io/dl/v${KIND_VERSION}/kind-linux-amd64
        chmod 777 /tmp/kind
        curl -fsSL -o /tmp/get_helm.sh https://raw.githubusercontent.com/helm/helm/master/scripts/get-helm-3
        chmod 700 /tmp/get_helm.sh
        HELM_INSTALL_DIR=/tmp/ /tmp/get_helm.sh -v v${HELM_VERSION} --no-sudo
        chmod 777 /tmp/helm
        curl -L https://dl.k8s.io/release/v${KUBECTL_VERSION}/bin/linux/amd64/kubectl -o /tmp/kubectl
        chmod 777 /tmp/kubectl
        curl https://dl.min.io/client/mc/release/linux-amd64/mc --create-dirs -o /tmp/mc
        chmod +x /tmp/mc
}

test_workflow(){
       local workflow="$1"

       echo "Testing $workflow" 
       DEPLOY_KUBEFLOW=1 make -C scripts/k8s-setup setup
       make -C $workflow workflow-test
       echo "Run workflow completed"
}

build_workflow(){
       local workflow="$1"

       echo "Build $workflow"
       make -C $workflow workflow-build
       echo "Build workflow completed"
}


usage(){
	echo "command not found"
}

case "$op" in
        install-tools)
                echo "install tool required for testing workflow"
                install_tools
                ;;
        test-workflow)
                echo "test workflow"
                test_workflow $2
                ;;
	build-workflow)
                echo "build workflow"
                build_workflow $2
		;;
        *)
                usage
                ;;
esac
