# go-ds-flatfs

[![](https://img.shields.io/badge/made%20by-Protocol%20Labs-blue.svg?style=flat-square)](http://ipn.io)
[![](https://img.shields.io/badge/project-IPFS-blue.svg?style=flat-square)](http://ipfs.io/)
[![](https://img.shields.io/badge/freenode-%23ipfs-blue.svg?style=flat-square)](http://webchat.freenode.net/?channels=%23ipfs)
[![standard-readme compliant](https://img.shields.io/badge/standard--readme-OK-green.svg?style=flat-square)](https://github.com/RichardLitt/standard-readme)
[![GoDoc](https://godoc.org/github.com/ipfs/go-ds-flatfs?status.svg)](https://godoc.org/github.com/ipfs/go-ds-flatfs)
[![Build Status](https://travis-ci.org/ipfs/go-ds-flatfs.svg?branch=master)](https://travis-ci.org/ipfs/go-ds-flatfs)
[![Coverage Status](https://img.shields.io/codecov/c/github/ipfs/go-ds-flatfs.svg)](https://codecov.io/gh/ipfs/go-ds-flatfs)


> A datastore implementation using sharded directories and flat files to store data

`go-ds-flatfs` is used by `go-ipfs` to store raw block contents on disk. It supports several sharding functions (prefix, suffix, next-to-last/*).

## Table of Contents

- [Install](#install)
- [Usage](#usage)
- [Contribute](#contribute)
- [License](#license)

## Install

`go-ds-flatfs` can be used like any Go module:


```
import "github.com/ipfs/go-ds-flatfs"
```

`go-ds-flatfs` uses [`Gx`](https://github.com/whyrusleeping/gx) and [`Gx-go`](https://github.com/whyrusleeping/gx-go) to handle dependendencies. Run `make deps` to download and rewrite the imports to their fixed dependencies.

## Usage

Check the [GoDoc module documentation](https://godoc.org/github.com/ipfs/go-ds-flatfs) for an overview of this module's
functionality.

### DiskUsage and accuracy

This datastore implements the [`PersistentDatastore`](https://godoc.org/github.com/ipfs/go-datastore#PersistentDatastore) interface. It offers a `DiskUsage()` method which strives to find a balance between accuracy and performance. This implies:

* The total disk usage of a datastore is calculated when opening the datastore
* The current disk usage is cached frequently in a file in the datastore root (`diskUsage.cache` by default). This file is also
written when the datastore is closed.
* If this file is not present when the datastore is opened:
  * The disk usage will be calculated by walking the datastore's directory tree and estimating the size of each folder.
  * This may be a very slow operation for huge datastores or datastores with slow disks
  * The operation is time-limited (5 minutes by default).
  * Upon timeout, the remaining folders will be assumed to have the average of the previously processed ones.
* After opening, the disk usage is updated in every write/delete operation.

This means that for certain datastores (huge ones, those with very slow disks or special content), the values reported by
`DiskUsage()` might be reduced accuracy and the first startup (without a `diskUsage.cache` file present), might be slow.

If you need increased accuracy or a fast start from the first time, you can replace the `diskUsage.cache` file (while the
datastore is not open), with the right disk usage value in size. I.e., in the datastore root:

    $ du -sb .
    3919232394        .
    $ echo -n "3919232394" > diskUsage.cache


## Contribute

PRs accepted.

Small note: If editing the README, please conform to the [standard-readme](https://github.com/RichardLitt/standard-readme) specification.

## License

MIT © Protocol Labs, Inc.
