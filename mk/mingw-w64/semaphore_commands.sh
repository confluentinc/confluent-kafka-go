#!/bin/bash
set -e
export HOME=/c/Users/$USER
export PATH=$HOME/go/bin:/C/msys64/mingw64/bin:$PATH
export MAKE=mingw32-make  # so that Autotools can find it
source .semaphore/semaphore_commands.sh
