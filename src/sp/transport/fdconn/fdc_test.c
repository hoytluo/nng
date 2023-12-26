//
// Copyright 2023 Staysail Systems, Inc. <info@staysail.tech>
// Copyright 2018 Capitar IT Group BV <info@capitar.com>
// Copyright 2018 Devolutions <info@devolutions.net>
// Copyright 2018 Cody Piersall <cody.piersall@gmail.com>
//
// This software is supplied under the terms of the MIT License, a
// copy of which should be located in the distribution where this
// file was obtained (LICENSE.txt).  A copy of the license may also be
// found online at https://opensource.org/licenses/MIT.
//

#include <nuts.h>

// FDC tests.
static void
test_fdc_connect_fail(void)
{
	nng_socket s;

	NUTS_OPEN(s);
	NUTS_FAIL(nng_dial(s, "fdconn://", NULL, 0), NNG_ENOTSUP);
	NUTS_CLOSE(s);
}

void
test_fdc_malformed_address(void)
{
	nng_socket s1;

	NUTS_OPEN(s1);
	NUTS_FAIL(nng_listen(s1, "fdconn://junk", NULL, 0), NNG_EADDRINVAL);
	NUTS_CLOSE(s1);
}

void
test_fdc_listen(void)
{
	nng_socket s1;

	NUTS_OPEN(s1);
	NUTS_PASS(nng_listen(s1, "fdconn://", NULL, 0));
	NUTS_CLOSE(s1);
}

void
test_fdc_accept(void)
{
	nng_socket s1, s2;
	nng_listener l;
	int fds[2];

	NUTS_PASS(nng_socket_pair(fds));
	// make sure we won't have to deal with SIGPIPE - EPIPE is better
	signal(SIGPIPE, SIG_IGN);
	NUTS_OPEN(s1);
	NUTS_OPEN(s2);
	NUTS_PASS(nng_listener_create(&l, s1, "fdconn://"));
	NUTS_PASS(nng_listener_start(l, 0));
	NUTS_PASS(nng_listener_set_int(l, NNG_OPT_FDC_FD, fds[0]));
	NUTS_SLEEP(10);
	NUTS_CLOSE(s1);
	close(fds[1]);
}


void
test_fdc_exchange(void)
{
	nng_socket s1, s2;
	nng_listener l1, l2;
	int fds[2];

	NUTS_PASS(nng_socket_pair(fds));
	// make sure we won't have to deal with SIGPIPE - EPIPE is better
	signal(SIGPIPE, SIG_IGN);
	NUTS_OPEN(s1);
	NUTS_OPEN(s2);
	NUTS_PASS(nng_listener_create(&l1, s1, "fdconn://"));
	NUTS_PASS(nng_listener_start(l1, 0));
	NUTS_PASS(nng_listener_set_int(l1, NNG_OPT_FDC_FD, fds[0]));
	NUTS_PASS(nng_listener_create(&l2, s2, "fdconn://"));
	NUTS_PASS(nng_listener_start(l2, 0));
	NUTS_PASS(nng_listener_set_int(l2, NNG_OPT_FDC_FD, fds[1]));
	NUTS_SLEEP(10);
	NUTS_SEND(s1, "hello");
	NUTS_RECV(s2, "hello");
	NUTS_SEND(s2, "there");
	NUTS_RECV(s1, "there");

	NUTS_CLOSE(s1);
	NUTS_CLOSE(s2);
	close(fds[1]);

}
void
test_fdc_recv_max(void)
{
	char         msg[256];
	char         buf[256];
	nng_socket   s0;
	nng_socket   s1;
	nng_listener l0;
	nng_listener l1;
	size_t       sz;
	int fds[2];

	NUTS_PASS(nng_socket_pair(fds));

	NUTS_OPEN(s0);
	NUTS_PASS(nng_socket_set_ms(s0, NNG_OPT_RECVTIMEO, 100));
	NUTS_PASS(nng_socket_set_size(s0, NNG_OPT_RECVMAXSZ, 200));
	NUTS_PASS(nng_listener_create(&l0, s0, "fdconn://"));
	NUTS_PASS(nng_socket_get_size(s0, NNG_OPT_RECVMAXSZ, &sz));
	NUTS_TRUE(sz == 200);
	NUTS_PASS(nng_listener_set_size(l0, NNG_OPT_RECVMAXSZ, 100));
	NUTS_PASS(nng_listener_start(l0, 0));
	NUTS_PASS(nng_listener_set_int(l0, NNG_OPT_FDC_FD, fds[0]));

	NUTS_OPEN(s1);
	NUTS_PASS(nng_listener_create(&l1, s1, "fdconn://"));
	NUTS_PASS(nng_listener_start(l1, 0));
	NUTS_PASS(nng_listener_set_int(l1, NNG_OPT_FDC_FD, fds[1]));
	NUTS_PASS(nng_send(s1, msg, 95, 0));
	NUTS_PASS(nng_socket_set_ms(s1, NNG_OPT_SENDTIMEO, 100));
	NUTS_PASS(nng_recv(s0, buf, &sz, 0));
	NUTS_TRUE(sz == 95);
	NUTS_PASS(nng_send(s1, msg, 150, 0));
	NUTS_FAIL(nng_recv(s0, buf, &sz, 0), NNG_ETIMEDOUT);
	NUTS_CLOSE(s0);
	NUTS_CLOSE(s1);
}

NUTS_TESTS = {

	{ "fdc connect fail", test_fdc_connect_fail },
	{ "fdc malformed address", test_fdc_malformed_address },
#ifdef NNG_HAVE_SOCKETPAIR
	{ "fdc listen", test_fdc_listen },
	{ "fdc accept", test_fdc_accept },
	{ "fdc exchange", test_fdc_exchange },
	{ "fdc recv max", test_fdc_recv_max },
#endif

	{ NULL, NULL },
};