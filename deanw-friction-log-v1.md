# Dean's "Friction Log" Notes

April 23, 2024

In preparation for open sourcing [`data-prep-lab`](https://github.com/IBM/data-prep-lab), I installed and ran through the getting-started content, looked at the docs, etc. Here are some notes, including any "friction" encountered.

## README

path: `./README.md`

### Installation

The steps in the README worked flawlessly. I started with a Conda environment using Python 3.11.

However, it mentions "if you will be using local Minio..." How do I know if I should use this or not??

### "Getting Started"

I like that there are several options, but don't forget to provide instructions for all of them, e.g., _Run a data pipeline on local-ray_ is still missing a link.

### "How to Contribute"

(Use consistent casing in section titles, so should it be "Contribute"?)

Since you have a `CONTRIBUTING.md` file, you could remove this section.

## About Apple Silicon `data-prep-kit/doc/mac.md`

The current problems with running on Apple Silicon need serious attention. Probably the majority of potential users will be working on these Macs, therefore they will find it off-putting that they have a degraded experience, whatever the reasons.

As I write this, I haven't gone through the examples yet, but they should all run easily on Apple Silicon Macs in the non-K8s mode, without emulation and `podman` or `docker`.

## Simplest Transform Tutorial

path: `data-prep-kit/data-processing-lib/doc/simplest-transform-tutorial.md`


## NOOP Transform

path: `data-prep-kit/transforms/universal/noop/README.md`

Can you provide more explanation about what the configuration values, do, e.g.,

* `noop_sleep_sec`: _specifies the number of seconds to sleep during table transformation._ What does "during" mean. Is it really before or after processing starts? Is this something "real" transformers might want, e.g., to allow something to happen in the background before reading starts?
* `noop_pwd` - _specifies a dummy password not included in metadata._ I assume this is for accessing the file. Is a user name also required?

Of course, you said these are examples, but I would go to extra effort to clarify specific behaviors, to encourage users to do the same.

As a general note about configuration parameter names, would it be better to use a hierarchy, like `noop.sleep.sec` so that in the `noop` context, you would just look at `sleep.sec` in the config file. I made `sleep` a parent in case you added more options, e.g., 

```
noop.sleep.enable = false
noop.sleep.sec = 2
```

[Running the "simplest" tutorial](https://github.com/IBM/data-prep-kit/blob/dev/data-processing-lib/doc/simplest-transform-tutorial.md#running) assumes IBM Cloud:

```
python noop_main.py --noop_sleep_msec 2 \
  --run_locally True  \
  --s3_cred "{'access_key': 'KEY', 'secret_key': 'SECRET', 'cos_url': 'https://s3.us-east.cloud-object-storage.appdomain.cloud'}" \
  --s3_config "{'input_folder': 'cos-optimal-llm-pile/test/david/input/', 'output_folder': 'cos-optimal-llm-pile/test/david/output/'}"
```

Some dummy test data in the repo is needed so users can use that instead. What would be the command-line flags for local storage?

It's fine with me to leave in an S3 (_true_ S3) example for those users so equipped, but otherwise, this is hardly a "local" example and I'm currently blocked.

Also, it appears you need to have moved into the `data-preprocessing-lib` directory and followed the setup instructions in that `README`, so your environment is setup. None of this is mentioned in this file. I recommend the top-level `README` file send you to that `README` file before discussing any of the examples.

I went through the `venv` setup in `data-prep-kit/data-processing-lib/README.md`, which worked fine, but then it was clear that the example code in this `README` has many errors, references to classes that don't exist, etc. I'm stopping here; please clean up this example page and I'll try again.

## Data Processing Library

path: `data-prep-kit/data-processing-lib/README.md`

This worked fine.
