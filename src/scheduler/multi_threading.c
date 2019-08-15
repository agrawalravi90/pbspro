/*
 * Copyright (C) 1994-2019 Altair Engineering, Inc.
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

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>
#include <errno.h>

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

void *worker(void* arg);


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
 * @brief	initialize multi-threading
 *
 * @param	void
 *
 * @return	int
 * @retval	1 for success
 * @retval	0 for malloc error
 */
int
init_multi_threading(void)
{
	if (threads == NULL) {
		int i;
		int num_cores;
		pthread_mutexattr_t attr;

		/* Create task and result queues */
		work_queue = new_ds_queue();
		if (work_queue == NULL) {
			log_err(errno, __func__, MEM_ERR_MSG);
			return 0;
		}
		result_queue = new_ds_queue();
		if (result_queue == NULL) {
			log_err(errno, __func__, MEM_ERR_MSG);
			return 0;
		}

		threads_die = 0;
		if (pthread_cond_init(&work_cond, NULL) != 0) {
			schdlog(PBSEVENT_ERROR, PBS_EVENTCLASS_SCHED, LOG_ERR, __func__,
					"pthread_cond_init failed");
			return 0;
		}
		if (pthread_cond_init(&result_cond, NULL) != 0) {
			schdlog(PBSEVENT_ERROR, PBS_EVENTCLASS_SCHED, LOG_ERR, __func__,
					"pthread_cond_init failed");
			return 0;
		}

		if (pthread_mutexattr_init(&attr) != 0) {
			schdlog(PBSEVENT_ERROR, PBS_EVENTCLASS_SCHED, LOG_ERR, __func__,
					"pthread_mutexattr_init failed");
			return 0;
		}
		if (pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE)) {
			schdlog(PBSEVENT_ERROR, PBS_EVENTCLASS_SCHED, LOG_ERR, __func__,
					"pthread_mutexattr_settype failed");
			return 0;
		}
		pthread_mutex_init(&work_lock, &attr);
		pthread_mutex_init(&result_lock, &attr);

		/* Create as many threads as the number of cores */
#ifdef WIN32
		SYSTEM_INFO sysinfo;
		GetSystemInfo(&sysinfo);
		num_cores = sysinfo.dwNumberOfProcessors;
#else
		num_cores = sysconf(_SC_NPROCESSORS_ONLN);
#endif
		num_threads = num_cores;
		threads = malloc(num_threads * sizeof(pthread_t));
		if (threads == NULL) {
			log_err(errno, __func__, MEM_ERR_MSG);
			return 0;
		}

		pthread_once(&key_once, create_id_key);
		for (i = 0; i < num_threads; i++) {
			int *thid;

			thid = malloc(sizeof(int));
			if (thid == NULL) {
				log_err(errno, __func__, MEM_ERR_MSG);
				return 0;
			}
			*thid = i + 1;
			pthread_create(&(threads[i]), NULL, &worker, (void *) thid);
		}
	}

	return 1;
}

/**
 * @brief	Main pthread routine for worker threads
 *
 * @param[in]	tid  - thread id of the thread
 *
 * @return void
 */
void *
worker(void *tid)
{
	th_task_info *work = NULL;
	void *ts = NULL;

	pthread_setspecific(th_id_key, tid);

	while (!threads_die) {
		/* Get the next work task from work queue */
		pthread_mutex_lock(&work_lock);
		while (ds_queue_is_empty(work_queue) && !threads_die) {
			pthread_cond_wait(&work_cond, &work_lock);
		}
		work = ds_dequeue(work_queue);
		pthread_mutex_unlock(&work_lock);

		/* find out what task we need to do */
		if (work != NULL) {
			switch (work->task_type) {
			case TS_IS_ND_ELIGIBLE:
				check_node_eligibility_chunk((th_data_nd_eligible *) work->thread_data);
				break;
			case TS_DUP_ND_INFO:
				dup_node_info_chunk((th_data_dup_nd_info *) work->thread_data);
				break;
			case TS_QUERY_ND_INFO:
				query_node_info_chunk((th_data_query_ninfo *) work->thread_data);
				break;
			case TS_FREE_ND_INFO:
				free_node_info_chunk((th_data_free_ninfo *) work->thread_data);
				break;
			case TS_DUP_RESRESV:
				dup_resource_resv_array_chunk((th_data_dup_resresv *) work->thread_data);
				break;
			case TS_QUERY_JOB_INFO:
				query_jobs_chunk((th_data_query_jinfo *) work->thread_data);
				break;
			case TS_FREE_RESRESV:
				free_resource_resv_array_chunk((th_data_free_resresv *) work->thread_data);
				break;
			default:
				schdlog(PBSEVENT_ERROR, PBS_EVENTCLASS_SCHED, LOG_ERR, __func__,
						"Invalid task type passed to worker thread");
			}

			/* Post results */
			pthread_mutex_lock(&result_lock);
			ds_enqueue(result_queue, (void *) work);
			pthread_cond_signal(&result_cond);
			pthread_mutex_unlock(&result_lock);
		}
	}

	ts = get_avl_tls();
	if (ts != NULL)
		free(ts);

	pthread_exit(NULL);
}
