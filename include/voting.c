#include "voting.h"
#include "utils.h"
#include "constants.h"
#include "network.h"
#include "memory.h"
#include "threads.h"
#include <unistd.h>
#include <stdio.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <string.h>
#include <inttypes.h>
#include <pthread.h>
#include <errno.h>
#include <stdarg.h>

#define SOCKET_TRIALS 3
#define ACCEPTOR_SOCKET_TRIALS 6
#define PROMISE_EXPIRY 2

struct args {
	int UID;
	const char* port_number;
	pthread_mutex_t * m;
	int * wait;
	pthread_cond_t * cond;
	pthread_mutex_t * em;
	int * exit;
	int nodes;
	int vote_ID;
	Arena * arena;
};

/**
 * This function returns the voted UID
 */
int conduct_voting(int UID, const char* port_number, int vote_ID, Arena * arena) {
	Arena * curArena = getLastFilledArena(arena);
	int arenaOffset = curArena->offset;

	// get number of nodes ready to vote
	int nodes = getNumNodes(vote_ID, port_number, arena);
	
	// first create two threads, one proposer and one acceptor
	pthread_t proposer_th, acceptor_th;
	pthread_mutex_t m = PTHREAD_MUTEX_INITIALIZER;
	pthread_mutex_t em = PTHREAD_MUTEX_INITIALIZER;
	pthread_cond_t cond = PTHREAD_COND_INITIALIZER;
	int wait = 0;
	int exit = 0;

	struct args * func_args = amalloc(arena, sizeof(struct args));
	func_args->UID = UID;
	func_args->port_number = port_number;
	func_args->m = &m;
	func_args->wait = &wait;
	func_args->cond = &cond;
	func_args->exit = &exit;
	func_args->em = &em;
	func_args->nodes = nodes;
	func_args->vote_ID = vote_ID;
	func_args->arena = arena;

	void * thread_result;

	create_thread(&proposer_th, NULL, proposer, (void *)func_args, 1, "[conduct_voting] Could not create proposer thread\n");
	create_thread(&acceptor_th, NULL, acceptor, (void *)func_args, 1, "[conduct_voting] Could not create acceptor thread\n");

	// wait for the acceptor and store the returned value (elected node) in a variable
	wait_for_thread(acceptor_th, &thread_result);
	// once the acceptor gives the result, wait for the proposer stop
	wait_for_thread(proposer_th, NULL);

	freeArenaPartial(curArena, arenaOffset);

	// check if the value returned is actually correct
	int elected_UID = (int)(intptr_t)thread_result;
	if (elected_UID <= 0) {
		return -1;
	}
	return elected_UID;
}

/**
 * This function gets the number of nodes willing to conduct this voting
 */
int getNumNodes(int vote_ID, const char* port_number, Arena * arena) {
	int yes = 1;
	struct timeval tv;
	tv.tv_sec = 0;
	tv.tv_usec = 10000;
	int fd = createSocket(
		port_number, 0, SOCK_DGRAM, 3,
		SOL_SOCKET, SO_REUSEADDR, &yes, sizeof yes,
		SOL_SOCKET, SO_BROADCAST, &yes, sizeof yes,
		SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv
	);

	struct sockaddr_in addr;
	set_broadcast_addr(port_number, &addr);
	socklen_t addr_len = sizeof(struct sockaddr_in);

	int tries = 0;
	int nodes = 0;
	int bufLen = sizeof(struct presence_packet);
	void * buf = amalloc(arena, bufLen);
	struct presence_packet * pp;

	while (tries < SOCKET_TRIALS) {
		int recved = recvfrom(fd, buf, bufLen, 0, &addr, &addr_len);
		if (recved == bufLen) {
			pp = (struct presence_packet *)buf;
			if (pp->packet_ID == PACKET_ID && pp->packet_type == PRESENCE_PACKET) {
				tries = 0;
				nodes += 1;
				continue;
			}
		}
		tries += 1;
	}

	return nodes;
}

struct accept_params {
	int * accepted_N;
	int * accepted_value;
	int vote_ID;
};

struct ack_params {
	int vote_ID;
};

void * accept_parse(va_list args, Arena * arena) {
	struct accept_params * ap = amalloc(arena, sizeof(struct accept_params));
	ap->accepted_N = va_arg(args, int *);
	ap->accepted_value = va_arg(args, int *);
	ap->vote_ID = va_arg(args, int);
	return (void *)ap;
}

void * ack_parse(va_list args, Arena * arena) {
	struct ack_params * ap = amalloc(arena, sizeof(struct ack_params));
	ap->vote_ID = va_arg(args, int);
	return (void *)ap;
}

int accept_eval(void * buf, void * params) {
	struct promise_packet * pro_p = (struct promise_packet *)buf;
	struct accept_params * ap = (struct accept_params *)params;
	int * accepted_N = ap->accepted_N;
	int * accepted_value = ap->accepted_value;
	int vote_ID = ap->vote_ID;

	// check if there was an accepted N already, if so just get the highest N returned
	// by all acceptors and store the accepted value of the highest returned N
	// also check if this is a packet of the intended voting
	// (since multiple distinct votings can happen in parallel)
	if (
		pro_p->packet_type == PROMISE_PACKET
		&& pro_p->accepted_N != -1
		&& pro_p->accepted_N > *accepted_N
		&& pro_p->vote_ID == vote_ID
	) {
		*accepted_N = pro_p->accepted_N;
		*accepted_value = pro_p->accepted_value;
		return EXIT_SUCCESS;
	}

	return EXIT_FAILURE;
}

int ack_eval(void * buf, void * params) {
	struct ack_packet * ap = (struct ack_packet *)buf;
	struct ack_params * ack_pa = (struct ack_params *)params;
	int vote_ID = ack_pa->vote_ID;

	// check if this is a packet of the ongoing vote
	// (since multiple votes can happen in parallel)
	// and it is an ack packet
	if (ap->packet_type == ACK_PACKET && ap->vote_ID == vote_ID) {
		return EXIT_SUCCESS;
	}

	return EXIT_FAILURE;
}

int recv_majority(
	int fd,
	int nodes,
	void * buf,
	int buf_size,
	struct sockaddr_in * recv_addr,
	int * recv_addr_size,
	void * (*parse)(va_list),
	int (*eval)(void * buf, void * params),
	int expected_packet,
	...
) {
	int replies = 0;
	int fails = 0;

	void * params;

	if (parse != NULL) {
		va_list args;
		va_start(args, expected_packet);
		params = parse(args);
		va_end(args);
	}

	int packet_ID = 0;

	while (replies < ((nodes / 2) + 1) && fails < SOCKET_TRIALS) {
		int recved = recvfrom(fd, buf, buf_size, 0, recv_addr, recv_addr_size);
		// check if this is a Code Arena Packet
		if (recved >= sizeof(int)) {
			memcpy(&packet_ID, buf, sizeof(int));
			if (packet_ID == PACKET_ID) {
				if (recved < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
					// socket timed out
					fails++;
				} else if (recved == buf_size && eval != NULL && eval(buf, params) == EXIT_SUCCESS) {
					fails = 0;
					replies++;
				}
			}
		}
	}

	return fails < SOCKET_TRIALS ? EXIT_SUCCESS : EXIT_FAILURE;
}

/**
 * This thread sends out prepare packets
 * Then listens for promise packets
 * Sends out accept packets
 * Listens for ack packets
 * Sends out commit packets
 * backoff for random delay when told my acceptor thread
 */
void* proposer(void* args) {
	#pragma region INIT

	struct args * arg = (struct args *)args;
	int UID = arg->UID;
	const char* port_number = arg->port_number;
	pthread_mutex_t * m = arg->m;
	int * wait = arg->wait;
	pthread_cond_t * cond = arg->cond;
	pthread_mutex_t * em = arg->em;
	int * exit = arg->exit;
	int nodes = arg->nodes;
	int vote_ID = arg->vote_ID;
	Arena * arena = arg->arena;

	// create request packet
	struct prepare_packet * pre_p = amalloc(arena, sizeof(struct prepare_packet));
	memset(pre_p, 0, sizeof(struct prepare_packet));
	pre_p->packet_ID = PACKET_ID;
	pre_p->vote_ID = vote_ID;
	pre_p->UID = UID;
	pre_p->packet_type = PREPARE_PACKET;

	// create promise packet
	struct promise_packet * pro_p = amalloc(arena, sizeof(struct promise_packet));
	memset(pro_p, 0, sizeof(struct promise_packet));

	// create accept packet
	struct accept_packet * acc_p = amalloc(arena, sizeof(struct accept_packet));
	memset(acc_p, 0, sizeof(struct accept_packet));
	acc_p->packet_ID = PACKET_ID;
	acc_p->vote_ID = vote_ID;
	acc_p->packet_type = ACCEPT_PACKET;

	// struct ack packet
	struct ack_packet * ack_p = amalloc(arena, sizeof(struct ack_packet));
	memset(ack_p, 0, sizeof(struct ack_packet));

	// struct commit packet
	struct commit_packet * com_p = amalloc(arena, sizeof(struct commit_packet));
	memset(com_p, 0, sizeof(struct commit_packet));
	com_p->packet_ID = PACKET_ID;
	com_p->vote_ID = vote_ID;
	com_p->packet_type = COMMIT_PACKET;

	int yes = 1;
	struct timeval tv;
	tv.tv_sec = 0;
	tv.tv_usec = 10000;
	int fd = createSocket("0", 0, SOCK_DGRAM, 3,
		SOL_SOCKET, SO_REUSEADDR, &yes, sizeof yes,
		SOL_SOCKET, SO_BROADCAST, &yes, sizeof yes,
		SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv
	);

	struct sockaddr_in addr;
	set_broadcast_addr(port_number, &addr);
	socklen_t addr_size = sizeof(struct sockaddr_in);

	struct sockaddr_in recv_addr;
	memset(&recv_addr, 0, sizeof(struct sockaddr_in));
	socklen_t recv_addr_size = sizeof(struct sockaddr_in);

	#pragma endregion

	#pragma region LOOP

	// this thread sends out voting packets
	int N = 1;
	while (1) {
		int val = 0;
		manipulate_value(&val, exit, sizeof(int), em);
		if (val == 1) {
			break;
		}

		// wait if told to
		int val = 1;
		manipulate_value_cond(wait, &val, sizeof(int), m, cond, 1);
		// pauseExecution(m, cond, wait);

		pre_p->N = N;

		#pragma region PHASE 1

		// send out request packet
		if (sendto(fd, pre_p, sizeof(struct prepare_packet), 0, &addr, addr_size) != -1) {

			// listen for majority
			int accepted_N = 0;
			int accepted_value = 0;
			int recved_majority = recv_majority(
				fd,
				nodes,
				(void *)pro_p,
				sizeof(struct promise_packet),
				&recv_addr,
				&recv_addr_size,
				accept_parse,
				accept_eval,
				&accepted_N,
				&accepted_value,
				vote_ID
			);

			// if majority was achieved only then proceed
			if (recved_majority == EXIT_SUCCESS) {
				#pragma region PHASE 2

				if (accepted_N == 0) {
					accepted_N = N;
					accepted_value = UID;
				}

				// acheived majority, proceed to phase 2, send accept packet
				acc_p->N = N;
				acc_p->value = accepted_value;
				int sentAccept = sendto(fd, acc_p, sizeof(struct accept_packet), 0, &addr, addr_size);

				if (sentAccept < 0) {
					// some error which should probably not occur since the previous send call was successful
					// Just continue if this error does occur anyway
					// The same value will be sent again the next time
					continue;
				}

				// now listen for ack packet
				int recved_ack_majority = recv_majority(
					fd,
					nodes,
					&ack_p,
					sizeof(struct ack_packet),
					&recv_addr,
					&recv_addr_size,
					ack_parse,
					ack_eval,
					ACK_PACKET,
					vote_ID
				);

				// only proceed if a majority is received
				if (recved_ack_majority == EXIT_SUCCESS) {
					#pragma region COMMIT

					// now send a commit message
					com_p->N = N;
					int sentCommit = sendto(fd, com_p, sizeof(struct commit_packet), 0, &addr, addr_size);

					if (sentCommit < 0) {
						// some error which should probably not occur since the previous send call was successful
						// Just continue if this error does occur anyway
						// Either this node or another node will commit the same value due to the design of paxos
						continue;
					}

					// just exit, the acceptors will know who is the chosen monitor at the end
					break;

					#pragma endregion
				}

				#pragma endregion
			}
		}

		#pragma endregion

		N++;
		// random backoff
		usleep(randInt(0, 100000));
	}

	#pragma endregion
}

/**
 * This thread listens for prepare packets
 * Sends out promise packets
 * Then listens for accept packets
 * Sends out ack packets
 * Listens for commit packets
 */
void* acceptor(void* args) {
	#pragma region INIT

	struct args * arg = (struct args *)args;
	int UID = arg->UID;
	const char* port_number = arg->port_number;
	pthread_mutex_t * m = arg->m;
	int * wait = arg->wait;
	pthread_cond_t * cond = arg->cond;
	pthread_mutex_t * em = arg->em;
	int * exit = arg->exit;
	int nodes = arg->nodes;
	int vote_ID = arg->vote_ID;
	Arena * arena = arg->arena;

	// create request packet
	struct prepare_packet * pre_p = amalloc(arena, sizeof(struct prepare_packet));
	memset(pre_p, 0, sizeof(struct prepare_packet));

	// create promise packet
	struct promise_packet * pro_p = amalloc(arena, sizeof(struct promise_packet));
	memset(pro_p, 0, sizeof(struct promise_packet));
	pro_p->packet_ID = PACKET_ID;
	pro_p->vote_ID = vote_ID;
	pro_p->packet_type = PROMISE_PACKET;

	// create accept packet
	struct accept_packet * acc_p = amalloc(arena, sizeof(struct accept_packet));
	memset(acc_p, 0, sizeof(struct accept_packet));

	// struct ack packet
	struct ack_packet * ack_p = amalloc(arena, sizeof(struct ack_packet));
	memset(ack_p, 0, sizeof(struct ack_packet));
	ack_p->packet_ID = PACKET_ID;
	ack_p->vote_ID = vote_ID;
	ack_p->packet_type = ACK_PACKET;

	// struct commit packet
	struct commit_packet * com_p = amalloc(arena, sizeof(struct commit_packet));
	memset(com_p, 0, sizeof(struct commit_packet));

	// unknown buffer with a size of the largest packet
	int bufSize = LARGEST_PACKET;
	void * buf = amalloc(arena, bufSize);
	memset(buf, 0, bufSize);

	int yes = 1;
	struct timeval tv;
	tv.tv_sec = 0;
	tv.tv_usec = 10000;
	int fd = createSocket(port_number, 0, SOCK_DGRAM, 3,
		SOL_SOCKET, SO_REUSEADDR, &yes, sizeof yes,
		SOL_SOCKET, SO_BROADCAST, &yes, sizeof yes,
		SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv
	);

	struct sockaddr_in recv_addr;
	memset(&recv_addr, 0, sizeof(struct sockaddr_in));
	socklen_t recv_addr_size = sizeof(struct sockaddr_in);

	int promised_N = 0;
	time_t promised_time = time(NULL);
	int accepted_N = 0;
	int accepted_value = 0;
	int commited_N = 0;
	int commited_value = -1;

	int tries = 0;

	int recved_packet_ID = 0;
	int recved_packet_type = 0;

	#pragma endregion

	#pragma region LOOP

	while (tries < ACCEPTOR_SOCKET_TRIALS) {
		// check for timeout every iteration
		if (time(NULL) > promised_time + PROMISE_EXPIRY) {
			promised_N = 0;
			// to prevent deadlocks
			int val = 0;
			manipulate_value_cond(wait, &val, sizeof(int), m, cond, 0);
			// controlExecution(m, cond, wait, 0);
		}

		int recved = recvfrom(fd, buf, bufSize, 0, &recv_addr, &recv_addr_size);

		if (recved > sizeof(int) * 2) {
			memcpy(&recved_packet_ID, buf, sizeof(int));
			memcpy(&recved_packet_type, buf + sizeof(int), sizeof(int));
			if (recved_packet_ID == PACKET_ID) {
				if (recved < 0
					&& (errno == EAGAIN || errno == EWOULDBLOCK)
				) {
					// timed out, increase the number of tries
					tries += 0;
				} else {
					tries = 0;
					if (recved_packet_type == PREPARE_PACKET && recved == sizeof(struct prepare_packet)) {
						memcpy(pre_p, buf, sizeof(struct prepare_packet));
						// check if the packet is valid and the current N is greater than accepted N and promised N
						if (pre_p->vote_ID == vote_ID && pre_p->N > accepted_N && pre_p->N > promised_N) {
							// send the accepted N and value
							pro_p->accepted_N = accepted_N;
							pro_p->accepted_value = accepted_value;
							promised_N = pre_p->N;
							promised_time = time(NULL);
							if (sendto(fd, pro_p, sizeof(struct promise_packet), 0, &recv_addr, recv_addr_size) < 0) {
								// promise not sent, handle the error
								// TODO Handle the error
							}
							// stop the proposer thread if not already proposed and to prevent future proposals
							// only if this node did not request
							int val = 1;
							if (pre_p->UID != UID) manipulate_value_cond(wait, &val, sizeof(int), m, cond, 0);
							// if (pre_p->UID != UID) controlExecution(m, cond, wait, 1);
						}
					} else if (recved_packet_type == ACCEPT_PACKET && recved == sizeof(struct accept_packet)) {
						memcpy(acc_p, buf, sizeof(struct accept_packet));
						// check if this the N that was promised earlier
						if (acc_p->vote_ID == vote_ID && acc_p->N == promised_N && promised_N > 0) {
							// update the accepted value
							accepted_N = acc_p->N;
							accepted_value = acc_p->value;
							// send the ack packet
							ack_p->N = accepted_N;
							// if send fails the node will try again to get a majority if it doesn't get majority
							sendto(fd, ack_p, sizeof(struct ack_packet), 0, &recv_addr, recv_addr_size);
						}
					} else if (recved_packet_type == COMMIT_PACKET && recved == sizeof(struct commit_packet)) {
						memcpy(com_p, buf, sizeof(struct commit_packet));
						if (com_p->vote_ID == vote_ID && com_p->N == accepted_N) {
							// commit the accepted value
							commited_N = accepted_N;
							commited_value = accepted_value;

							break;
						}
					}
				}
			}
		}
	}

	int val = 1;
	manipulate_value(exit, &val, sizeof(int), em);
	return (void *)(intptr_t)commited_value;

	#pragma endregion
}
