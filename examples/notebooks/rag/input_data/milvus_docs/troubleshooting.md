---
id: troubleshooting.md
summary: Learn about common issues you may encounter with Milvus and how to overcome them.
title: Troubleshooting
---
# Troubleshooting
This page lists common issues that may occur when running Milvus, as well as possible troubleshooting tips. Issues on this page fall into the following categories:

- [Boot issues](#boot_issues)
- [Runtime issues](#runtime_issues)
- [API issues](#api_issues)
- [etcd crash issues](#etcd_crash_issues)


  ## Boot issues

  Boot errors are usually fatal. Run the following command to view error details:

  ```
  $ docker logs <your milvus container id>
  ```


  ## Runtime issues

  Errors that occur during runtime may cause service breakdown. To troubleshoot this issue, check compatibility between the server and your client before moving forward.


  ## API issues

  These issues occur during API method calls between the Milvus server and your client. They will be returned to the client synchronously or asynchronously.
  

  ## etcd crash issues
  
  ### 1. etcd pod pending

  The etcd cluster uses pvc by default. StorageClass needs to be preconfigured for the Kubernetes cluster.

  ### 2. etcd pod crash

  When an etcd pod crashes with `Error: bad member ID arg (strconv.ParseUint: parsing "": invalid syntax), expecting ID in Hex`, you can log into this pod and delete the `/bitnami/etcd/data/member_id` file.

  ### 3. Multiple pods keep crashing while `etcd-0` is still running

  You can run the following code if multiple pods keeps crashing while `etcd-0` is still running.
  
  ```
  kubectl scale sts <etcd-sts> --replicas=1
  # delete the pvc for etcd-1 and etcd-2
  kubectl scale sts <etcd-sts> --replicas=3
  ```
  
  ### 4. All pods crash
  
  When all pods crash, try copying the `/bitnami/etcd/data/member/snap/db` file. Use `https://github.com/etcd-io/bbolt` to modify database data.

  All Milvus metadata are kept in the `key` bucket. Back up the data in this bucket and run the following commands. Note that the prefix data in the `by-dev/meta/session` file does not require a backup.
  
  ```
  kubectl kubectl scale sts <etcd-sts> --replicas=0
  # delete the pvc for etcd-0, etcd-1, etcd-2
  kubectl kubectl scale sts <etcd-sts> --replicas=1
  # restore the backup data
  ```



<br/>

  If you need help solving a problem, feel free to:

  - Join our [Slack channel](https://join.slack.com/t/milvusio/shared_invite/enQtNzY1OTQ0NDI3NjMzLWNmYmM1NmNjOTQ5MGI5NDhhYmRhMGU5M2NhNzhhMDMzY2MzNDdlYjM5ODQ5MmE3ODFlYzU3YjJkNmVlNDQ2ZTk) and reach out for support from the Milvus team.
  - [File an Issue](https://github.com/milvus-io/milvus/issues/new/choose) on GitHub that includes details about your problem.

