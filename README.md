#Firmament

Firmament is a cluster manager and scheduling platform. Firmament is currently in alpha stage: it runs your jobs and tasks fairly
reliably, but system components keep changing regularly, and we are actively
developing system features.

# Getting started

The easiest way to get Firmament up and running is to use our Docker image:

```
$ docker pull huaweifirmament/firmament
```

Once the image has downloaded, you can start Firmament as follows:

```console
$ docker run --net=host huaweifirmament/firmament /firmament/build/src/firmament_scheduler --flagfile=/firmament/config/firmament_scheduler_cpu_mem.cfg
```

# Building from source

## System requirements

Firmament is currently known to work on Ubuntu LTS releases 12.04 (precise) and
14.04 (trusty). With caveats (see below), it works on 13.04 (raring) and 13.10
(saucy); it does NOT work on other versions prior to 12.10 (quantal) as they
cannot build libpion, which is now included as a self-built dependency in order
to ease transition to libpion v5 and for compatibility with Arch Linux.
Packages required for Ubuntu to build Firmament are listed in files pkglist.Ubuntu-<release_version> [here](https://github.com/Huawei-PaaS/firmament/tree/dev/include).

Other configurations are untested - YMMV. Recent Debian versions typically work
with a bit of fiddling of the build configuration files in the `include`
directory.

Reasons for known breakage:
 * Ubuntu 13.04 - segfault failures when using Boost 1.53 packages; use 1.49
                  (default).
 * Ubuntu 13.10 - clang/LLVM include paths need to be fixed.
                  /usr/{lib,include}/clang/3.2 should be symlinked to
                  /usr/lib/llvm-3.2/lib/clang/3.2.

## Building instructions

After cloning the repository,

```console
$ mkdir build
$ cd build
$ cmake ..
$ make
```

This fetches and builds dependencies are necessary, although CMake may ask you to
install required packages and libraries.

```console
$ ctest
```

runs unit tests.

Binaries are in the build/src subdirectory of the project root, and all accept
the `--helpshort` argument to show their command line options.

You can bring up the firmament by running below command.

```
$ cd firmament
$ ./build/src/firmament_scheduler --flagfile=config/firmament_scheduler.cfg
```

## Using the flow scheduler

By default, Firmament starts up with a simple queue-based scheduler. If you want
to instead use our new scheduler based on flow network optimization, pass
the `--scheduler flow` flag to the coordinator on startup:

```console
$ build/src/coordinator --scheduler flow --flow_scheduling_cost_model 6 --listen_uri tcp:<host>:<port> --task_lib_dir=$(pwd)/build/src
```

The `--flow_scheduling_cost_model` option choses the cost model on which the
scheduler's flow network is based: here, we specify a simple load-balacing model
that aims to put the same number of tasks on each machine. Several other cost
models are available and in development.

### Cost models

There are currently eight scheduling policies ("cost models") in the Firmament
code base:

| Cost model  | Description                                               | Status   |
| ----------- | --------------------------------------------------------- | -------- |
| TRIVIAL (0) | Fixed costs, tasks always schedule if resources are idle. | Complete |
| RANDOM (1)  | Random costs, for fuzz tests. Not useful in practice!     | Complete |
| SJF (2)     | Shortest job first policy based on avg. past runtimes.    | Complete |
| QUINCY (3)  | Original Quincy cost model, with data locality.           | Complete |
| WHARE (4)   | Implementation of Whare-Map's M and MCs policies.         | Complete |
| COCO (5)    | Coordinated co-location model (in development).           | Complete |
| OCTOPUS (6) | Simple load balancing based on task counts.               | Complete |
| VOID (7)    | Bogus cost model used for KB with simple scheduler.       | Complete |
| NET-BW (8)  | Network-bandwidth-aware cost model (avoids hotspots).     | Complete |
| CPU-MEM (10) | Task placement based on CPU and MEM request.             | Complete |

## Running on multiple machines

To use Firmament across multiple machines, you need to run a `coordinator`
instance on each machine. These coordinators can then be arranged in a tree
hierarchy, in which each coordinator can schedule tasks locally _and_ on its
subordinate childrens' resources.

To run a coordinator as a child of a parent coordinator, pass the `--parent_uri`
flag on launch and set it to the parent coordinator's network location:
```console
$ build/src/coordinator --listen_uri tcp:<local host>:<local port> --parent_uri tcp:<parent host>:<parent port> --task_lib_dir=$(pwd)/build/src/
```
The parent coordinator must already be running. Once both coordinators are up,
you will be able to see the child resources on the parent coordinator's web UI.

# Running the Firmament scheduler service

The easiest way to get Firmament scheduler service up and running is to use our Docker image:

Once the image has downloaded, you can start the Firmament scheduler service as follows:

```
$ docker run huaweifirmament/firmament /firmament/build/src/firmament_scheduler --flagfile=/firmament/default.conf
```

You can now deploy our Firmament integration with Kubernetes to schedule Kubernetes pods.
You can follow these [instructions](https://github.com/kubernetes-sigs/poseidon/blob/master/README.md)
to build and deploy Poseidon, our Kubernetes integration.


# Contributing

We always welcome contributions to Firmament. One contribution you can
easily make as a newcomer is to do **code reviews** -- this also helps you
familiarise yourself with the Firmament code base, _en passant_.

If you would like to contribute a **pull request**, that's also most welcome!

### Code style

We follow the [Google C++ style guide](https://google.github.io/styleguide/cppguide.html)
in the Firmament code base. A subset of the style guide's rules can be verified
using the `make lint` target, which runs the C++ linting script on your
checkout.

## Contact
If you would like to contact us, please create an issue on GitHub.
