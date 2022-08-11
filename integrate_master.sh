#!/bin/bash

set -x

git remote add upstream https://github.com/feast-dev/feast.git

git fetch upstream

git checkout master
git rebase upstream/master
