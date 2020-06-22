/*
 * Copyright (C) 1994-2020 Altair Engineering, Inc.
 * For more information, contact Altair at www.altair.com.
 *
 * This file is part of both the OpenPBS software ("OpenPBS")
 * and the PBS Professional ("PBS Pro") software.
 *
 * Open Source License Information:
 *
 * OpenPBS is free software. You can redistribute it and/or modify it under
 * the terms of the GNU Affero General Public License as published by the
 * Free Software Foundation, either version 3 of the License, or (at your
 * option) any later version.
 *
 * OpenPBS is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Affero General Public
 * License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * Commercial License Information:
 *
 * PBS Pro is commercially licensed software that shares a common core with
 * the OpenPBS software.  For a copy of the commercial license terms and
 * conditions, go to: (http://www.pbspro.com/agreement.html) or contact the
 * Altair Legal Department.
 *
 * Altair's dual-license business model allows companies, individuals, and
 * organizations to create proprietary derivative works of OpenPBS and
 * distribute them - whether embedded or bundled with other software -
 * under a commercial license agreement.
 *
 * Use of Altair's trademarks, including but not limited to "PBS™",
 * "OpenPBS®", "PBS Professional®", and "PBS Pro™" and Altair's logos is
 * subject to Altair's trademark licensing policies.
 */


#include <pbs_config.h>

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>
#include <errno.h>
#include <signal.h>
#include <malloc.h>

#include "log.h"
#include "avltree.h"

#include "constant.h"
#include "misc.h"
#include "node_info.h"
#include "data_types.h"
#include "globals.h"
#include "node_info.h"
#include "queue.h"
#include "fifo.h"
#include "resource_resv.h"
#include "multi_threading.h"


/**
 * @brief	initialize a mutex attr object
 *
 * @param[out]	attr - the attr object to initialize
 *
 * @return int
 * @retval 1 for Success
 * @retval 0 for Error
 */
int
init_mutex_attr_recursive(pthread_mutexattr_t *attr)
{
	if (pthread_mutexattr_init(attr) != 0) {
		log_event(PBSEVENT_ERROR, PBS_EVENTCLASS_SCHED, LOG_ERR, __func__,
				"pthread_mutexattr_init failed");
		return 0;
	}

	if (pthread_mutexattr_settype(attr,
#if defined (linux)
			PTHREAD_MUTEX_RECURSIVE_NP
#else
			PTHREAD_MUTEX_RECURSIVE
#endif
	)) {
		log_event(PBSEVENT_ERROR, PBS_EVENTCLASS_SCHED, LOG_ERR, __func__,
				"pthread_mutexattr_settype failed");
		return 0;
	}

	return 1;
}

/**
 * @brief	create the thread id key & set it for the main thread
 *
 * @param	void
 *
 * @return	void
 */
static void
create_id_key(void)
{
	int *mainid;

	mainid = malloc(sizeof(int));
	if (mainid == NULL) {
		log_err(errno, __func__, MEM_ERR_MSG);
		return;
	}
	*mainid = 0;

	pthread_key_create(&th_id_key, free);
	pthread_setspecific(th_id_key, (void *) mainid);
}

/**
 * @brief	convenience function to kill worker threads
 *
 * @param	void
 *
 * @return	void
 */
void
kill_threads(void)
{
	int i;

	log_event(PBSEVENT_DEBUG, PBS_EVENTCLASS_REQUEST, LOG_DEBUG,
				"", "Killing worker threads");

	/* Wait until all threads to finish */
	for (i = 0; i < num_threads; i++) {
		pthread_join(threads[i], NULL);
	}

	pthread_mutex_destroy(&general_lock);
	free(threads);
	threads = NULL;
	num_threads = 0;
}

/**
 * @brief	initialize multi-threading
 *
 * @param[in]	nthreads - number of threads to create, or -1 to use default
 *
 * @return	int
 * @retval	1 for success
 * @retval	0 for malloc error
 */
int
init_multi_threading(int nthreads)
{
	int i;
	int num_cores;
	pthread_mutexattr_t attr;

	/* Kill any existing worker threads */
	if (num_threads > 1)
		kill_threads();

	if (init_mutex_attr_recursive(&attr) == 0)
		return 0;

	pthread_mutex_init(&general_lock, &attr);

	num_cores = sysconf(_SC_NPROCESSORS_ONLN);
	if (nthreads < 1 && num_cores > 2)
		/* Create as many threads as half the number of cores */
		num_threads = num_cores / 2;
	else
		num_threads = nthreads;

	if (num_threads <= 1) {
		num_threads = 1;
		return 1; /* main thread will act as the only worker thread */
	}

	threads = malloc(num_threads * sizeof(pthread_t));
	if (threads == NULL) {
		log_err(errno, __func__, MEM_ERR_MSG);
		return 0;
	}

	pthread_once(&key_once, create_id_key);

	return 1;
}

/**
 * @brief	Convenience function to queue up work for worker threads
 *
 * @param[in]	task - the task to queue up
 *
 * @return void
 */
void
queue_work_for_threads(th_task_info *task)
{
	pthread_mutex_lock(&work_lock);
	ds_enqueue(work_queue, (void *) task);
	pthread_cond_signal(&work_cond);
	pthread_mutex_unlock(&work_lock);
}
