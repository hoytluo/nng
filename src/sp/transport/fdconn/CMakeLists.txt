#
# Copyright 2023 Staysail Systems, Inc. <info@staysail.tech>
#
# This software is supplied under the terms of the MIT License, a
# copy of which should be located in the distribution where this
# file was obtained (LICENSE.txt).  A copy of the license may also be
# found online at https://opensource.org/licenses/MIT.
#

# File Descriptor (or Handle) based connections
nng_directory(fdconn)

nng_sources(fdconn.c)
# nng_headers_if(NNG_TRANSPORT_FDCONN nng/transport/fdconn/fdconn.h)
nng_defines(NNG_TRANSPORT_FDCONN)
nng_test(fdc_test)