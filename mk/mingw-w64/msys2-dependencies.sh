#!/bin/bash

set -e

export msys2='cmd //C RefreshEnv.cmd '
export msys2+='& set MSYS=winsymlinks:nativestrict '
export msys2+='& C:\\msys64\\msys2_shell.cmd -defterm -no-start'
export mingw64="$msys2 -mingw64 -full-path -here -c "\"\$@"\" --"
export msys2+=" -msys2 -c "\"\$@"\" --"

# Have to update pacman first or choco upgrade will failure due to migration
# to zstd instead of xz compression
$msys2 pacman -Sy --noconfirm pacman

## Install more MSYS2 packages from https://packages.msys2.org/base here
$msys2 pacman --sync --noconfirm --needed mingw-w64-x86_64-gcc
