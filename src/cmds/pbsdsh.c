/*
 * Copyright (C) 1994-2020 Altair Engineering, Inc.
 * For more information, contact Altair at www.altair.com.
 *
 * This file is part of the PBS Professional ("PBS Pro") software.
 *
 * Open Source License Information:
 *
 * PBS Pro is free software. You can redistribute it and/or modify it under the
 * terms of the GNU Affero General Public License as published by the Free
 * Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * PBS Pro is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * Commercial License Information:
 *
 * For a copy of the commercial license terms and conditions,
 * go to: (http://www.pbspro.com/UserArea/agreement.html)
 * or contact the Altair Legal Department.
 *
 * Altair’s dual-license business model allows companies, individuals, and
 * organizations to create proprietary derivative works of PBS Pro and
 * distribute them - whether embedded or bundled with other software -
 * under a commercial license agreement.
 *
 * Use of Altair’s trademarks, including but not limited to "PBS™",
 * "PBS Professional®", and "PBS Pro™" and Altair’s logos is subject to Altair's
 * trademark licensing policies.
 *
 */
/**
 * @file	pbs_dsh.c
 * @brief
 * pbs_dsh - a distribute task program using the Task Management API
 *
 */

#include <pbs_config.h>   /* the master config generated by configure */
#include <pbs_version.h>

#include "cmds.h"
#include "tm.h"
#include <signal.h>

int	   *ev;
tm_event_t *events_spawn;
tm_event_t *events_obit;
int	    numnodes;
tm_task_id *tid;
int	    verbose = 0;

#ifndef WIN32
sigset_t	allsigs;
#endif

char		*id;

int	fire_phasers = 0;
int	no_obit = 0;
extern char *get_ecname(int rc);

/**
 * @brief
 *	signal handler function
 *
 * @param[in] sig - signal number
 *
 * @return - Void
 *
 */
void
bailout(int sig)
{
	fire_phasers = sig;
}

/**
 * @brief
 *	wait_for_task - wait for all spawned tasks to
 *	a. have the spawn acknowledged, and
 *	b. the task to terminate and return the obit with the exit status
 *
 * @param[in] first - first event index to consider
 * @param[in] nspawned - number of tasks spawned
 *
 * @return - Void
 *
 */
void
wait_for_task(int first, int *nspawned)
{
	int	    c;
	tm_event_t  eventpolled;
	int	    nevents;
	int	    nobits = 0;
	int	    rc;
	int	    tm_errno;

	nevents = *nspawned;
	while (*nspawned || nobits) {
		if (verbose) {
			printf("pbsdsh: waiting on %d spawned and %d obits\n",
				*nspawned, nobits);
		}

		if (fire_phasers) {
			tm_event_t	event;

			for (c=first; c<(first+nevents); c++) {
				if (*(tid+c) == TM_NULL_TASK)
					continue;
				printf("pbsdsh: killing task 0x%08X signal %d\n",
					*(tid+c), fire_phasers);
				(void)tm_kill(*(tid+c), fire_phasers, &event);
			}
			tm_finalize();
			exit(1);
		}


#ifdef WIN32
		rc = tm_poll(TM_NULL_EVENT, &eventpolled, 1, &tm_errno);
#else
		sigprocmask(SIG_UNBLOCK, &allsigs, NULL);
		rc = tm_poll(TM_NULL_EVENT, &eventpolled, 1, &tm_errno);
		sigprocmask(SIG_BLOCK, &allsigs, NULL);
#endif

		if (rc != TM_SUCCESS) {
			fprintf(stderr, "%s: Event poll failed, error %s\n",
				id, get_ecname(rc));
			exit(2);
		}

		for (c = first; c < (first+nevents); ++c) {
			if (eventpolled == *(events_spawn + c)) {
				/* spawn event returned - register obit */
				(*nspawned)--;
				if (tm_errno) {
					fprintf(stderr, "error %d on spawn\n",
						tm_errno);
					continue;
				}
				if (no_obit)
					continue;

				rc = tm_obit(*(tid+c), ev+c, events_obit+c);
				if (rc == TM_SUCCESS) {
					if (*(events_obit+c) == TM_NULL_EVENT) {
						if (verbose) {
							fprintf(stderr, "task already dead\n");
						}
					} else if (*(events_obit+c) == TM_ERROR_EVENT) {
						if (verbose) {
							fprintf(stderr, "Error on Obit return\n");
						}
					} else {
						nobits++;
					}
				} else if (verbose) {
					fprintf(stderr, "%s: failed to register for task termination notice, task 0x%08X\n", id, c);
				}


			} else if (eventpolled == *(events_obit + c)) {
				/* obit event, task exited */
				nobits--;
				*(tid+c) = TM_NULL_TASK;
				if (verbose || *(ev+c) != 0) {
					printf("%s: task 0x%08X exit status %d\n",
						id, c, *(ev+c));
				}
			}
		}
	}
}

int
main(int argc, char *argv[], char *envp[])
{
	int                     c = 0;
	int                     err = 0;
	int                     max_events;
	int                     ncopies = -1;
	int                     nd = 0;
	int                     onenode = -1;
	int                     rc = 0;
	struct tm_roots         rootrot;
	int	                nspawned = 0;
	tm_node_id              *nodelist = NULL;
	int                     start = 0;
	int                     stop = 0;
	int                     sync = 0;
	char                    *pbs_environ = NULL;
#ifndef WIN32
	struct	sigaction	act;
#endif
	extern int   optind;
	extern char *optarg;

	/*test for real deal or just version and exit*/

	PRINT_VERSION_AND_EXIT(argc, argv);

#ifdef WIN32
	if (winsock_init()) {
		return 1;
	}
#endif
	while ((c = getopt(argc, argv, "c:n:svo")) != EOF) {
		switch (c) {
			case 'c':
				ncopies = atoi(optarg);
				if (ncopies < 0) {
					err = 1;
				}
				break;
			case 'n':
				onenode = atoi(optarg);
				if (onenode < 0) {
					err = 1;
				}
				break;
			case 's':
				sync = 1;		/* force synchronous spawns */
				break;
			case 'v':
				verbose = 1;	/* turn on verbose output */
				break;
			case 'o':
				no_obit = 1;
				break;
			default:
				err = 1;
				break;
		}
	}
	if (err || (onenode >= 0 && ncopies >= 0) || (argc == optind)) {
		fprintf(stderr, "Usage: %s [-c copies][-s][-v][-o]"
			" -- program [args...]\n", argv[0]);
		fprintf(stderr, "       %s [-n node_index][-s][-v][-o]"
			" -- program [args...]\n", argv[0]);
		fprintf(stderr, "       %s --version\n", argv[0]);
		fprintf(stderr, "Where -c copies =  run a copy "
			"of \"program\" on the first \"copies\" nodes,\n");
		fprintf(stderr, "      -n node_index = run a copy "
			"of \"program\" on the \"node_index\"-th node,\n");

		fprintf(stderr, "      -s = forces synchronous execution,\n");
		fprintf(stderr, "      -v = forces verbose output.\n");
		fprintf(stderr, "      -o = no obits are waited for.\n");

		exit(1);
	}

	id = argv[0];
	if ((pbs_environ = getenv("PBS_ENVIRONMENT")) == 0) {
		fprintf(stderr, "%s: not executing under PBS\n", id);
		return 1;
	}

	/*
	 *	Set up interface to the Task Manager
	 */
	if ((rc = tm_init(0, &rootrot)) != TM_SUCCESS) {
		fprintf(stderr, "%s: tm_init failed, rc = %s (%d)\n", id,
			get_ecname(rc), rc);
		return 1;
	}

#ifdef WIN32
	signal(SIGINT, bailout);
	signal(SIGTERM, bailout);
#else
	sigemptyset(&allsigs);
	sigaddset(&allsigs, SIGHUP);
	sigaddset(&allsigs, SIGINT);
	sigaddset(&allsigs, SIGTERM);

	act.sa_mask = allsigs;
	act.sa_flags = 0;
	/*
	 ** We want to abort system calls and call a function.
	 */
#ifdef	SA_INTERRUPT
	act.sa_flags |= SA_INTERRUPT;
#endif
	act.sa_handler = bailout;
	sigaction(SIGHUP, &act, NULL);
	sigaction(SIGINT, &act, NULL);
	sigaction(SIGTERM, &act, NULL);

#endif	/* WIN32 */

#ifdef DEBUG
	if (rootrot.tm_parent == TM_NULL_TASK) {
		printf("%s: I am the mother of all tasks\n", id);
	} else {
		printf("%s: I am but a child in the scheme of things\n", id);
	}
#endif /* DEBUG */

	if ((rc = tm_nodeinfo(&nodelist, &numnodes)) != TM_SUCCESS) {
		fprintf(stderr, "%s: tm_nodeinfo failed, rc = %s (%d)\n", id,
			get_ecname(rc), rc);
		return 1;
	}

	max_events = (ncopies > numnodes) ? ncopies : numnodes;

	/* malloc space for various arrays based on number of nodes/tasks */

	tid = (tm_task_id *)calloc(max_events, sizeof(tm_task_id));
	if (tid == NULL) {
		fprintf(stderr, "%s: malloc of task ids failed\n", id);
		return 1;
	}
	events_spawn = (tm_event_t *)calloc(max_events, sizeof(tm_event_t));
	if (events_spawn == NULL) {
		fprintf(stderr, "%s: out of memory\n", id);
		return 1;
	}
	events_obit  = (tm_event_t *)calloc(max_events, sizeof(tm_event_t));
	if (events_obit == NULL) {
		fprintf(stderr, "%s: out of memory\n", id);
		return 1;
	}
	ev = (int *)calloc(max_events, sizeof(int));
	if (ev == NULL) {
		fprintf(stderr, "%s: out of memory\n", id);
		return 1;
	}
	for (c = 0; c < max_events; c++) {
		*(tid + c)          = TM_NULL_TASK;
		*(events_spawn + c) = TM_NULL_EVENT;
		*(events_obit  + c) = TM_NULL_EVENT;
		*(ev + c)	    = 0;
	}


	/* Now spawn the program to where it goes */

	if (onenode >= 0) {

		/* Spawning one copy onto logical node "onenode" */

		start = onenode;
		stop  = onenode + 1;

	} else if (ncopies >= 0) {
		/* Spawn a copy of the program to the first "ncopies" nodes */

		start = 0;
		stop  = ncopies;
	} else {
		/* Spawn a copy on all nodes */

		start = 0;
		stop  = numnodes;
	}

#ifndef WIN32
	sigprocmask(SIG_BLOCK, &allsigs, NULL);
#endif

	for (c = 0; c < (stop-start); ++c) {
		nd = (start + c) % numnodes;
		if ((rc = tm_spawn(argc-optind,
			argv+optind,
			NULL,
			*(nodelist + nd),
			tid + c,
			events_spawn + c)) != TM_SUCCESS) {
			fprintf(stderr, "%s: spawn failed on node %d err %s\n",
				id, nd, get_ecname(rc));
		} else {
			if (verbose)
				printf("%s: spawned task 0x%08X on logical node %d event %d\n", id, c, nd, *(events_spawn+c));
			++nspawned;
			if (sync)
				wait_for_task(c, &nspawned); /* one at a time */
		}

	}

	if (sync == 0)
		wait_for_task(0, &nspawned);	/* wait for all to finish */
#ifdef WIN32
	/*
	 * On Windows, in case of interactive jobs - pbs_demux is writing on stdout and stderr
	 * in parallel to the interactive shell on which pbsdsh executes. Give pbs_demux some time to
	 * finish writing to the console before we exit from here.
	 *
	 */
	if (strncmp(pbs_environ, "PBS_INTERACTIVE", strlen("PBS_INTERACTIVE")) == 0) {
		Sleep(200);/* 200 ms */
	}
#endif
	/*
	 *	Terminate interface with Task Manager
	 */
	tm_finalize();

	return 0;
}
