//
// Copyright 2023 Staysail Systems, Inc. <info@staysail.tech>
// Copyright 2018 Capitar IT Group BV <info@capitar.com>
// Copyright 2019 Devolutions <info@devolutions.net>
//
// This software is supplied under the terms of the MIT License, a
// copy of which should be located in the distribution where this
// file was obtained (LICENSE.txt).  A copy of the license may also be
// found online at https://opensource.org/licenses/MIT.
//

#include "core/nng_impl.h"

#include <errno.h>
#include <poll.h>
#include <stdbool.h>
#include <sys/uio.h>
#include <unistd.h>

#include "core/fdconn.h"
#include "platform/posix/posix_aio.h"

struct nni_fdc_conn {
	nng_stream      stream;
	nni_posix_pfd * pfd;
	nni_list        readq;
	nni_list        writeq;
	bool            closed;
	nni_mtx         mtx;
	nni_reap_node   reap;
};

static void
fdc_dowrite(nni_fdc_conn *c)
{
	nni_aio *aio;
	int      fd;

	if (c->closed || ((fd = nni_posix_pfd_fd(c->pfd)) < 0)) {
		return;
	}

	while ((aio = nni_list_first(&c->writeq)) != NULL) {
		unsigned      i;
		int           n;
		int           niov;
		unsigned      naiov;
		nni_iov *     aiov;
		struct iovec  iovec[16];

		nni_aio_get_iov(aio, &naiov, &aiov);

		if (naiov > NNI_NUM_ELEMENTS(iovec)) {
			nni_aio_list_remove(aio);
			nni_aio_finish_error(aio, NNG_EINVAL);
			continue;
		}

		for (niov = 0, i = 0; i < naiov; i++) {
			if (aiov[i].iov_len > 0) {
				iovec[niov].iov_len  = aiov[i].iov_len;
				iovec[niov].iov_base = aiov[i].iov_buf;
				niov++;
			}
		}

		if ((n = writev(fd, iovec, niov)) < 0) {
			switch (errno) {
			case EINTR:
				continue;
			case EAGAIN:
#ifdef EWOULDBLOCK
#if EWOULDBLOCK != EAGAIN
			case EWOULDBLOCK:
#endif
#endif
				return;
			default:
				nni_aio_list_remove(aio);
				nni_aio_finish_error(
				    aio, nni_plat_errno(errno));
				return;
			}
		}

		// If we didn't send all the data, the caller will
		// resubmit.  As a corollary, callers should probably
		// only have one message on the write queue at a time.
		nni_aio_bump_count(aio, n);
		nni_aio_list_remove(aio);
		nni_aio_finish(aio, 0, nni_aio_count(aio));

		// Go back to start of loop to see if there is another
		// aio ready for us to process.
	}
}

static void
fdc_doread(nni_fdc_conn *c)
{
	nni_aio *aio;
	int      fd;

	if (c->closed || ((fd = nni_posix_pfd_fd(c->pfd)) < 0)) {
		return;
	}

	while ((aio = nni_list_first(&c->readq)) != NULL) {
		unsigned     i;
		int          n;
		int          niov;
		unsigned     naiov;
		nni_iov *    aiov;
		struct iovec iovec[16];

		nni_aio_get_iov(aio, &naiov, &aiov);
		if (naiov > NNI_NUM_ELEMENTS(iovec)) {
			nni_aio_list_remove(aio);
			nni_aio_finish_error(aio, NNG_EINVAL);
			continue;
		}
		for (niov = 0, i = 0; i < naiov; i++) {
			if (aiov[i].iov_len != 0) {
				iovec[niov].iov_len  = aiov[i].iov_len;
				iovec[niov].iov_base = aiov[i].iov_buf;
				niov++;
			}
		}

		if ((n = readv(fd, iovec, niov)) < 0) {
			switch (errno) {
			case EINTR:
				continue;
			case EAGAIN:
				return;
			default:
				nni_aio_list_remove(aio);
				nni_aio_finish_error(
				    aio, nni_plat_errno(errno));
				return;
			}
		}

		if (n == 0) {
			// Zero indicates a closed descriptor.
			// This implicitly completes this (all!) aio.
			nni_aio_list_remove(aio);
			nni_aio_finish_error(aio, NNG_ECONNSHUT);
			continue;
		}

		nni_aio_bump_count(aio, n);

		// We completed the entire operation on this aio.
		nni_aio_list_remove(aio);
		nni_aio_finish(aio, 0, nni_aio_count(aio));

		// Go back to start of loop to see if there is another
		// aio ready for us to process.
	}
}

static void
fdc_error(void *arg, int err)
{
	nni_fdc_conn *c = arg;
	nni_aio *     aio;

	nni_mtx_lock(&c->mtx);
	while (((aio = nni_list_first(&c->readq)) != NULL) ||
	    ((aio = nni_list_first(&c->writeq)) != NULL)) {
		nni_aio_list_remove(aio);
		nni_aio_finish_error(aio, err);
	}
	if (c->pfd != NULL) {
		nni_posix_pfd_close(c->pfd);
	}
	nni_mtx_unlock(&c->mtx);
}

static void
fdc_close(void *arg)
{
	nni_fdc_conn *c = arg;
	nni_mtx_lock(&c->mtx);
	if (!c->closed) {
		nni_aio *aio;
		c->closed = true;
		while (((aio = nni_list_first(&c->readq)) != NULL) ||
		    ((aio = nni_list_first(&c->writeq)) != NULL)) {
			nni_aio_list_remove(aio);
			nni_aio_finish_error(aio, NNG_ECLOSED);
		}
		if (c->pfd != NULL) {
			nni_posix_pfd_close(c->pfd);
		}
	}
	nni_mtx_unlock(&c->mtx);
}

// fdc_fini may block briefly waiting for the pollq thread.
// To get that out of our context, we simply reap this.
static void
fdc_fini(void *arg)
{
	nni_fdc_conn *c = arg;
	fdc_close(c);
	if (c->pfd != NULL) {
		nni_posix_pfd_fini(c->pfd);
	}
	nni_mtx_fini(&c->mtx);

	NNI_FREE_STRUCT(c);
}

static nni_reap_list fdc_reap_list = {
	.rl_offset = offsetof(nni_fdc_conn , reap),
	.rl_func   = fdc_fini,
};
static void
fdc_free(void *arg)
{
	struct nni_fdc_conn *c = arg;
	nni_reap(&fdc_reap_list, c);
}

static void
fdc_cb(nni_posix_pfd *pfd, unsigned events, void *arg)
{
	struct nni_fdc_conn *c = arg;

	if (events & (NNI_POLL_HUP | NNI_POLL_ERR | NNI_POLL_INVAL)) {
		fdc_error(c, NNG_ECONNSHUT);
		return;
	}
	nni_mtx_lock(&c->mtx);
	if ((events & NNI_POLL_IN) != 0) {
		fdc_doread(c);
	}
	if ((events & NNI_POLL_OUT) != 0) {
		fdc_dowrite(c);
	}
	events = 0;
	if (!nni_list_empty(&c->writeq)) {
		events |= NNI_POLL_OUT;
	}
	if (!nni_list_empty(&c->readq)) {
		events |= NNI_POLL_IN;
	}
	if ((!c->closed) && (events != 0)) {
		nni_posix_pfd_arm(pfd, events);
	}
	nni_mtx_unlock(&c->mtx);
}

static void
fdc_cancel(nni_aio *aio, void *arg, int rv)
{
	nni_fdc_conn *c = arg;

	nni_mtx_lock(&c->mtx);
	if (nni_aio_list_active(aio)) {
		nni_aio_list_remove(aio);
		nni_aio_finish_error(aio, rv);
	}
	nni_mtx_unlock(&c->mtx);
}

static void
fdc_send(void *arg, nni_aio *aio)
{
	nni_fdc_conn *c = arg;
	int           rv;

	if (nni_aio_begin(aio) != 0) {
		return;
	}
	nni_mtx_lock(&c->mtx);

	if ((rv = nni_aio_schedule(aio, fdc_cancel, c)) != 0) {
		nni_mtx_unlock(&c->mtx);
		nni_aio_finish_error(aio, rv);
		return;
	}
	nni_aio_list_append(&c->writeq, aio);

	if (nni_list_first(&c->writeq) == aio) {
		fdc_dowrite(c);
		// If we are still the first thing on the list, that
		// means we didn't finish the job, so arm the poller to
		// complete us.
		if (nni_list_first(&c->writeq) == aio) {
			nni_posix_pfd_arm(c->pfd, POLLOUT);
		}
	}
	nni_mtx_unlock(&c->mtx);
}

static void
fdc_recv(void *arg, nni_aio *aio)
{
	nni_fdc_conn *c = arg;
	int           rv;

	if (nni_aio_begin(aio) != 0) {
		return;
	}
	nni_mtx_lock(&c->mtx);

	if ((rv = nni_aio_schedule(aio, fdc_cancel, c)) != 0) {
		nni_mtx_unlock(&c->mtx);
		nni_aio_finish_error(aio, rv);
		return;
	}
	nni_aio_list_append(&c->readq, aio);

	// If we are only job on the list, go ahead and try to do an
	// immediate transfer. This allows for faster completions in
	// many cases.  We also need not arm a list if it was already
	// armed.
	if (nni_list_first(&c->readq) == aio) {
		fdc_doread(c);
		// If we are still the first thing on the list, that
		// means we didn't finish the job, so arm the poller to
		// complete us.
		if (nni_list_first(&c->readq) == aio) {
			nni_posix_pfd_arm(c->pfd, POLLIN);
		}
	}
	nni_mtx_unlock(&c->mtx);
}

static const nni_option fdc_options[] = {
	{
	    .o_name = NULL,
	},
};

static int
fdc_get(void *arg, const char *name, void *buf, size_t *szp, nni_type t)
{
	nni_fdc_conn *c = arg;
	return (nni_getopt(fdc_options, name, c, buf, szp, t));
}

static int
fdc_set(void *arg, const char *name, const void *buf, size_t sz, nni_type t)
{
	nni_fdc_conn *c = arg;
	return (nni_setopt(fdc_options, name, c, buf, sz, t));
}

int
nni_fdc_conn_alloc(nni_fdc_conn **cp, int fd)
{
	nni_fdc_conn *c;
	int rv;
	if ((c = NNI_ALLOC_STRUCT(c)) == NULL) {
		return (NNG_ENOMEM);
	}
	if ((rv = nni_posix_pfd_init(&c->pfd, fd)) != 0) {
		NNI_FREE_STRUCT(c);
		return (rv);
	}

	c->closed = false;

	nni_mtx_init(&c->mtx);
	nni_aio_list_init(&c->readq);
	nni_aio_list_init(&c->writeq);

	c->stream.s_free  = fdc_free;
	c->stream.s_close = fdc_close;
	c->stream.s_recv  = fdc_recv;
	c->stream.s_send  = fdc_send;
	c->stream.s_get   = fdc_get;
	c->stream.s_set   = fdc_set;

	nni_posix_pfd_set_cb(c->pfd, fdc_cb, c);

	*cp = c;
	return (0);
}

void
nni_fdc_close_fd(int fd)
{
	close(fd);
}