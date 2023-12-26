//
// Copyright 2023 Staysail Systems, Inc. <info@staysail.tech>
//
// This software is supplied under the terms of the MIT License, a
// copy of which should be located in the distribution where this
// file was obtained (LICENSE.txt).  A copy of the license may also be
// found online at https://opensource.org/licenses/MIT.
//

#include <stdint.h>
#include <string.h>

#include <nng/nng.h>

#include "core/fdconn.h"
#include "core/nng_impl.h"

// We will accept up to this many FDs to be queued up for
// accept, before we start rejecting with NNG_ENOSPC.  Once
// accept is performed, then another slot is available.
#define NNG_FDC_LISTEN_QUEUE 16

int
nni_fdc_dialer_alloc(nng_stream_dialer **dp, const nng_url *url)
{
	NNI_ARG_UNUSED(dp);
	NNI_ARG_UNUSED(url);
	// No dialer support for this.
	return (NNG_ENOTSUP);
}

typedef struct {
	nng_stream_listener ops;
	int                 listen_cnt; // how many FDs are waiting
	int                 listen_q[NNG_FDC_LISTEN_QUEUE];
	bool                closed;
	nni_list            accept_q;
	nni_mtx             mtx;
} fdc_listener;

static void
fdc_listener_free(void *arg)
{
	fdc_listener *l = arg;
	nni_mtx_fini(&l->mtx);
	NNI_FREE_STRUCT(l);
}

static void
fdc_listener_close(void *arg)
{
	nni_aio      *aio;
	fdc_listener *l = arg;
	nni_mtx_lock(&l->mtx);
	l->closed = true;
	while ((aio = nni_list_first(&l->accept_q)) != NULL) {
		nni_aio_list_remove(aio);
		nni_aio_finish_error(aio, NNG_ECLOSED);
	}
	for (int i = 0; i < l->listen_cnt; i++) {
		nni_fdc_close_fd(l->listen_q[i]);
	}
	nni_mtx_unlock(&l->mtx);
}

static int
fdc_listener_listen(void *arg)
{
	NNI_ARG_UNUSED(arg);
	// nothing really for us to do
	return (0);
}

static void
fdc_start_conn(fdc_listener *l, nni_aio *aio)
{
	int           fd;
	int           rv;
	nni_fdc_conn *c;
	NNI_ASSERT(l->listen_cnt > 0);
	fd = l->listen_q[0];
	for (int i = 1; i < l->listen_cnt; i++) {
		l->listen_q[i] = l->listen_q[i + 1];
	}
	l->listen_cnt--;
	if ((rv = nni_fdc_conn_alloc(&c, fd)) != 0) {
		nni_aio_finish_error(aio, rv);
		nni_fdc_close_fd(fd);
	} else {
		nni_aio_set_output(aio, 0, c);
		nni_aio_finish(aio, 0, 0);
	}
}

static void
fdc_cancel_accept(nni_aio *aio, void *arg, int rv)
{
	fdc_listener *l = arg;

	nni_mtx_lock(&l->mtx);
	if (nni_aio_list_active(aio)) {
		nni_aio_list_remove(aio);
		nni_aio_finish_error(aio, rv);
	}
	nni_mtx_unlock(&l->mtx);
}

static void
fdc_listener_accept(void *arg, nng_aio *aio)
{
	fdc_listener *l = arg;

	if (nni_aio_begin(aio) != 0) {
		return;
	}
	nni_mtx_lock(&l->mtx);
	if (l->closed) {
		nni_mtx_unlock(&l->mtx);
		nni_aio_finish_error(aio, NNG_ECLOSED);
		return;
	}

	if (l->listen_cnt) {
		fdc_start_conn(l, aio);
	} else {
		nni_aio_schedule(aio, fdc_cancel_accept, l);
		nni_aio_list_append(&l->accept_q, aio);
	}
	nni_mtx_unlock(&l->mtx);
}

static int
fdc_listener_set_fd(void *arg, const void *buf, size_t sz, nni_type t)
{
	fdc_listener *l = arg;
	nni_aio      *aio;
	int           fd;
	int           rv;

	if ((rv = nni_copyin_int(&fd, buf, sz, NNI_MININT, NNI_MAXINT, t)) !=
	    0) {
		return (rv);
	}

	nni_mtx_lock(&l->mtx);
	if (l->closed) {
		nni_mtx_unlock(&l->mtx);
		return (NNG_ECLOSED);
	}

	if (l->listen_cnt == NNG_FDC_LISTEN_QUEUE) {
		nni_mtx_unlock(&l->mtx);
		return (NNG_ENOSPC);
	}

	l->listen_q[l->listen_cnt++] = fd;

	// if someone was waiting in accept, give it to them now
	if ((aio = nni_list_first(&l->accept_q)) != NULL) {
		nni_aio_list_remove(aio);
		fdc_start_conn(l, aio);
	}

	nni_mtx_unlock(&l->mtx);
	return (0);
}

static const nni_option fdc_listener_options[] = {
	{
	    .o_name = NNG_OPT_FDC_FD,
	    .o_set  = fdc_listener_set_fd,
	},
	{
	    .o_name = NULL,
	},
};

int
fdc_listener_get(
    void *arg, const char *name, void *buf, size_t *szp, nni_type t)
{
	fdc_listener *l = arg;
	return (nni_getopt(fdc_listener_options, name, l, buf, szp, t));
}

int
fdc_listener_set(
    void *arg, const char *name, const void *buf, size_t sz, nni_type t)
{
	fdc_listener *l = arg;
	return (nni_setopt(fdc_listener_options, name, l, buf, sz, t));
}

int
nni_fdc_listener_alloc(nng_stream_listener **lp, const nng_url *url)
{
	fdc_listener *l;

	NNI_ARG_UNUSED(url);

	if ((l = NNI_ALLOC_STRUCT(l)) == NULL) {
		return (NNG_ENOMEM);
	}
	memset(l->listen_q, 0, sizeof(l->listen_q));
	l->listen_cnt = 0;
	nni_aio_list_init(&l->accept_q);
	nni_mtx_init(&l->mtx);

	l->ops.sl_free   = fdc_listener_free;
	l->ops.sl_close  = fdc_listener_close;
	l->ops.sl_listen = fdc_listener_listen;
	l->ops.sl_accept = fdc_listener_accept;
	l->ops.sl_get    = fdc_listener_get;
	l->ops.sl_set    = fdc_listener_set;

	*lp = (void *) l;
	return (0);
}
