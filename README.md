# Least Load Plugin

[![Build Status](https://ci.jenkins.io/job/Plugins/job/leastload-plugin/job/master/badge/icon)](https://ci.jenkins.io/job/Plugins/job/leastload-plugin/job/master/)
[![Jenkins Plugin](https://img.shields.io/jenkins/plugin/v/leastload.svg)](https://plugins.jenkins.io/leastload/)
[![Changelog](https://img.shields.io/github/v/tag/jenkinsci/leastload-plugin?label=changelog)](https://github.com/jenkinsci/leastload-plugin/blob/master/CHANGELOG.md)
[![Jenkins Plugin Installs](https://img.shields.io/jenkins/plugin/i/leastload.svg?color=blue)](https://plugins.jenkins.io/leastload/)

## Overview

By default Jenkins tries to allocate a jobs to the last node is was executed on. This can result in nodes
being left idle while other nodes are overloaded. This plugin overrides the default behaviour and assigns jobs
to nodes with the least load. The least load is defined as a node that is idle or the one with the most available
executors.

It is intended to provide the same functionality as the commercial [Even Scheduler
Plugin](https://docs.cloudbees.com/docs/admin-resources/latest/plugins/even-scheduler).

## Configuration

The plugin requires no global configuration. It will simply replace the default load balancer at startup. Jobs can
be configured to utilize the default Jenkins load balancer if required.

![](docs/images/useDefault.png)
