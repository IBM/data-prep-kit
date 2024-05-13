## &#x2757; Execution on laptops with Apple Silicon (CPU)
Starting with certain models introduced in late 2020, Apple began the transition from Intel processors to Apple silicon in Mac computers.
These CPUs have ARM architecture and are incompatible with Intel processors. 

### Transforms
Developing transforms for either the [python or Ray runtimes](../data-processing-lib/doc/transform-runtimes.md), without KubeFlow pipelines (KFP), should have no issues on Apple silicon Macs,
or other platforms for that matter.
Therefore, to the extent the supported versions of python are used, transforms can be developed that will run on Apple silicon Macs. 
### Virtualization Considerations

Desktops such as [Docker Desktop](https://www.docker.com/products/docker-desktop/),
[Podman desktop](https://podman-desktop.io/) and [Rancher desktop](https://docs.rancherdesktop.io/) use different virtualization and emulation technics,
([qemu](https://www.qemu.org/), [Apple Virtualization framework](https://developer.apple.com/documentation/virtualization))
to allow the execution of containers based on images compiled for Intel silicon. However, emulation significantly
impacts performance, and there are additional restrictions, such as Virtual Machine RAM size.

On the other hand, executing a Kind Kubernetes cluster with KubeFlow pipelines (KFP) and local data storage (Minio)
requires a significant amount of memory. For this initial Data Prep Kit release, we do not recommend local (Kind)
execution on Mac computers with Apple silicon. Instead, we suggest using a real Kubernetes cluster or a Linux virtual
machine with an Intel CPU.

> **Note**: the *current* release does not support building cross-platform images, therefore, please do not build images 
on the Apple silicon. 
