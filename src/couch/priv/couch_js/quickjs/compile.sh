#!/bin/bash

# Test compile for development
# -Wno-format-truncation \
#
gcc -g -flto -O2 -Wall -MMD -Wno-array-bounds \
    -Iquickjs \
    -D_GNU_SOURCE -DCONFIG_BIGNUM=0 -DCONFIG_VERSION=\"quickjs\" \
    -DCONFIG_LTO \
    quickjs/cutils.c quickjs/libbf.c quickjs/libregexp.c quickjs/libunicode.c quickjs/quickjs-libc.c quickjs/quickjs.c main.c \
    -o main \
    -lm


# -fno-string-normalize -fno-map -fno-promise -fno-typedarray -fno-typedarray -fno-regexp -fno-json -fno-eval -fno-proxy -fno-date -m -o examples/hello_module examples/hello_module.js
