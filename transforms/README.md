# Transforms
This is the root director of all transforms.  It is organized as follows

* universal - holds transforms that may be useful across many domains
* language - holds transforms that are specific to language-based modeling 
* code - holds transforms that are specific to code-based modeling 

Each of these directories contains a number of directories, each directory implementing a specific transform.
Each transform is expected to be a standalone entity that generally runs at scale from within a docker image.

Please see the set of [project conventions](transform-conventions.md) used across these projects.
