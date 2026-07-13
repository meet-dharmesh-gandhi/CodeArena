#include "constants.h"
#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/fcntl.h>
#include <sys/random.h>
#include <sys/socket.h>
#include <sys/timerfd.h>

int randInt(int fallback, int max) {
	unsigned int num;

	ssize_t result = getrandom(&num, sizeof(int), 0);

	if (result < 0) {
		return fallback;
	}

	return num % max;
}

/**
 * Gives a small jitter time between `minJitter`
 * and `maxJitter`
 */
int getJitter(int maxJitter, int minJitter) {
	return randInt(minJitter, maxJitter - minJitter) + minJitter;
}

/**
 * This function reads and writes a shared variable using mutexes.
 */
void manipulate_value(void *dest, const void *src, size_t n,
					  pthread_mutex_t *m) {
	if (dest == NULL || src == NULL) {
		return;
	}

	pthread_mutex_lock(m);
	memcpy(dest, src, n);
	pthread_mutex_unlock(m);
}

/**
 * This function reads and writes a shared variable using mutex and condition
 * variables. when cond_wait is 1 to 6 the pthread_cond_wait function is
 * executed (blocking). And when cond_wait is any other value
 * pthread_cond_signal is executed. It waits while src <operator> dest.
 * <operator> is == if cond_wait is 1
 * <operator> is != if cond_wait is 2
 * <operator> is < if cond_wait is 3
 * <operator> is > if cond_wait is 4
 * <operator> is <= if cond_wait is 5
 * <operator> is >= if cond_wait is 6
 * When signaling, it writes src to dest
 */
void manipulate_value_cond(void *dest, const void *src, size_t n,
						   pthread_mutex_t *m, pthread_cond_t *cond,
						   int cond_wait) {
	if (dest == NULL || src == NULL) {
		return;
	}

	pthread_mutex_lock(m);
	memcpy(dest, src, n);

	if (cond != NULL) {
		switch (cond_wait) {
		case 1:
			while (memcmp(src, dest, n) == 0) {
				pthread_cond_wait(cond, m);
			}
			break;
		case 2:
			while (memcmp(src, dest, n) != 0) {
				pthread_cond_wait(cond, m);
			}
			break;
		case 3:
			while (memcmp(src, dest, n) < 0) {
				pthread_cond_wait(cond, m);
			}
			break;
		case 4:
			while (memcmp(src, dest, n) > 0) {
				pthread_cond_wait(cond, m);
			}
			break;
		case 5:
			while (memcmp(src, dest, n) <= 0) {
				pthread_cond_wait(cond, m);
			}
			break;
		case 6:
			while (memcmp(src, dest, n) >= 0) {
				pthread_cond_wait(cond, m);
			}
			break;
		default:
			pthread_cond_signal(cond);
			break;
		}
	}

	pthread_mutex_unlock(m);
}

int divideCeil(int numerator, int denominator) {
	return (numerator < 0) == (denominator < 0)
			   ? (numerator / denominator)
			   : ((numerator / denominator) - 1);
}

int divideFloor(int numerator, int denominator) {
	return (numerator < 0) == (denominator < 0)
			   ? ((numerator / denominator) + 1)
			   : (numerator / denominator);
}

int limit(int num, int lowest, int highest) {
	return num < lowest ? lowest : num > highest ? highest : num;
}

int min(int a, int b) { return a < b ? a : b; }

int max(int a, int b) { return a > b ? a : b; }

int get_index(uint64_t uid, int length) {
	return (uid * 11400714819323198485llu) % length;
}

time_t getCurrTime() {
	struct timespec ts;
	if (clock_gettime(CLOCK_REALTIME, &ts) == 0) {
		return (time_t)((ts.tv_sec * 1000) + ts.tv_nsec / (1000 * 1000));
	}
	return 0;
}

int setNonBlocking(int fd) {
	int flags = fcntl(fd, F_GETFL, 0);
	if (flags == -1) {
		return EXIT_FAILURE;
	}

	fcntl(fd, F_SETFL, flags | O_NONBLOCK);
	return EXIT_SUCCESS;
}

/**
 * gets a new timerfd with `interval` and `period`
 * If isMs is 0, the interval and period are
 * treated as in nanoseconds
 * If isMs is 1, the interval and period are
 * treated as in milliseconds
 */
int getNewTimerFD(clockid_t __clock_id, long interval, long period, int isMs) {
	int timerfd = timerfd_create(__clock_id, TFD_NONBLOCK);
	struct itimerspec utmr;
	utmr.it_value.tv_sec = 0;
	utmr.it_value.tv_nsec = interval * (isMs ? 1000000 : 1);
	utmr.it_interval.tv_sec = 0;
	utmr.it_interval.tv_nsec = period * (isMs ? 1000000 : 1);
	timerfd_settime(timerfd, 0, &utmr, NULL);

	return timerfd;
}

int createTimerFD(clockid_t __clock_id, long interval, long period, int isMs) {
	int timerfd = timerfd_create(__clock_id, TFD_NONBLOCK);

	return timerfd;
}

void startTimerFD(int timerfd, long interval, long period, int isMs) {
	struct itimerspec utmr;
	utmr.it_value.tv_sec = 0;
	utmr.it_value.tv_nsec = interval * (isMs ? 1000000 : 1);
	utmr.it_interval.tv_sec = 0;
	utmr.it_interval.tv_nsec = period * (isMs ? 1000000 : 1);
	timerfd_settime(timerfd, 0, &utmr, NULL);
}

void stopTimerFD(int timerfd) {
	struct itimerspec utmr;
	utmr.it_value.tv_sec = 0;
	utmr.it_value.tv_nsec = 0;
	utmr.it_interval.tv_sec = 0;
	utmr.it_interval.tv_nsec = 0;
	timerfd_settime(timerfd, 0, &utmr, NULL);
}

int getNewSocket(const char *port, suseconds_t tv_usec, int type) {
	int sock;
	if (type == SOCK_DGRAM) {
		int yes = 1;
		struct timeval tv;
		tv.tv_sec = 0;
		tv.tv_usec = tv_usec;
		sock =
			createSocket(port, 0, SOCK_DGRAM, 3, SOL_SOCKET, SO_REUSEADDR, &yes,
						 sizeof yes, SOL_SOCKET, SO_BROADCAST, &yes, sizeof yes,
						 SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);
	} else if (type == SOCK_STREAM) {
		int yes = 1;
		struct linger sl;
		sl.l_onoff = 0;
		sl.l_linger = 0;
		sock = createSocket(port, STREAM_WAITING_QUEUE, SOCK_STREAM, 3,
							SOL_SOCKET, SO_REUSEADDR, &yes, sizeof yes,
							SOL_SOCKET, SO_BROADCAST, &yes, sizeof yes,
							SOL_SOCKET, SO_LINGER, &sl, sizeof(sl));
	} else {
		return -1;
	}
	int success = setNonBlocking(sock);
	if (success == EXIT_SUCCESS) {
		return sock;
	} else {
		return -1;
	}
}