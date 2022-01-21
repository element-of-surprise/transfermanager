![Smarter TransferManagers](https://www.avato-consulting.com/wp-content/uploads/2019/10/smartdatamethode-1.jpg)

[![GoDoc](https://godoc.org/github.com/element-of-surprise/transfermanager?status.svg)](https://godoc.org/github.com/element-of-surprise/transfermanager) 

Please :star: this project

# Introduction

This repository holds additional AzBlob TransferManager(s) that provide more robust options than the basic managers that are either a sync.Pool implementation or a static number of buffers.

When I wrote those into azblob, I was simply providing the existing capabilities. One of the reasons I went for a TransferManager type was the hope that we could write more complex versions to help control the memory/upload speed in a more intelligent way.

Here you will find some new experimental TransferManager(s) that might be pushed into the main repo at some point.

## Static Overflow TransferManager

The first additional manager is called staticoverflow and can be used in multiple ways:
* Static buffer allocation
* Dynamic buffer with max limit
* Dynamic buffer without a limit
* Static buffer with dynamic overflow with limit
* Static buffer with dynamic overflow no limit

In addition, staticoverflow provides:
* A Stat() method to allow pulling stats to gauge runtime information about the manager to know how it is performing. You can use this to help determine
if other settings would provide benefit
* A Reset() method to adjust the settings in order to find optimal settings

## Future Plans

I plan to eventually create a maxtransfer TransferManager that will use staticoverflow to try to automatically adjust for maximum transfer speed. This is great for use cases when you want to do bulk transfers and just want it to push as hard as it can.

I may make another one that will limit the transfer speed to some number to prevent saturating a network.
