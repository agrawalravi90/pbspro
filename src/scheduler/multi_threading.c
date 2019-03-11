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

#include "log.h"

#include "buckets.h"
#include "check.h"
#include "data_types.h"
#include "globals.h"
#include "job_info.h"
#include "misc.h"
#include "node_info.h"
#include "queue.h"
#include "resource_resv.h"

void *worker(void* arg);
void *check_job_can_run(checkjob_thread_data *thread_data);

/**
 * @brief	initialize multi-threading
 *
 * @param	void
 *
 * @return	int
 * @retval	0 for success
 * @retval	1 for malloc error
 */
int
init_multi_threading(void)
{
	if (threads == NULL) {
		int i;
		int num_cores;

		/* Create task and result queues */
		work_queue = new_queue();
		result_queue = new_queue();
		if (work_queue == NULL || result_queue == NULL) {
			snprintf(log_buffer, sizeof(log_buffer), "Error malloc'ing thread memory");
			schdlog(PBSEVENT_ERROR, PBS_EVENTCLASS_SCHED, LOG_ERR, __func__, log_buffer);
			return 1;
		}

		threads_die = 0;
		pthread_cond_init(&work_cond, NULL);
		pthread_cond_init(&result_cond, NULL);
		pthread_mutex_init(&work_lock, NULL);
		pthread_mutex_init(&result_lock, NULL);

		/* Create as many threads as the number of cores */
#ifdef WIN32
		SYSTEM_INFO sysinfo;
		GetSystemInfo(&sysinfo);
		num_cores = sysinfo.dwNumberOfProcessors;
#else
		num_cores = sysconf(_SC_NPROCESSORS_ONLN);
#endif
		threads = malloc(num_cores * sizeof(pthread_t));
		if (threads == NULL) {
			snprintf(log_buffer, sizeof(log_buffer), "Error malloc'ing thread memory");
			schdlog(PBSEVENT_ERROR, PBS_EVENTCLASS_SCHED, LOG_ERR, __func__, log_buffer);
			return 1;
		}
		for (i = 0; i < num_cores; i++) {
			pthread_create(&(threads[i]), NULL, &worker, NULL);
		}
		num_threads = num_cores;
	}

	return 0;
}

void *
worker(void *arg)
{
	th_task_info *work = NULL;

	while (!threads_die) {
		/* Get the next work task from work queue */
		pthread_mutex_lock(&work_lock);
		while (is_empty(work_queue) && !threads_die) {
			pthread_cond_wait(&work_cond, &work_lock);
		}
		work = dequeue(work_queue);
		pthread_mutex_unlock(&work_lock);

		/* find out what task we need to do */
		if (work != NULL) {
			switch (work->task_type) {
			case TS_CHECKJOB:
				check_job_can_run((checkjob_thread_data *) work->thread_data);
				break;
			default:
				snprintf(log_buffer, sizeof(log_buffer), "Invalid task type passed to worker thread");
				schdlog(PBSEVENT_ERROR, PBS_EVENTCLASS_SCHED, LOG_ERR, __func__, log_buffer);
			}

			/* Post results */
			if (!threads_die) {
				pthread_mutex_lock(&result_lock);
				enqueue(result_queue, work);
				pthread_mutex_unlock(&result_lock);
				pthread_cond_signal(&result_cond);
			}
		}
	}

	pthread_exit(NULL);
}

/**
 * @brief	Thread local function, tries to see if a job can run by calling
 * 			is_ok_to_run(). If job can't run, it also does preemption simulation
 * 			to generate the first list of jobs to preempt
 *
 * @param[in]	data - checkjob_thread_data type object containing data for thread
 *
 * @return void *
 * @retval pointer to the node spec array if the job can run
 * @retval NULL if the is_ok_to_run returns NULL
 */
void *
check_job_can_run(checkjob_thread_data *thread_data)
{
	queue_info *qinfo;
	nspec **ns_arr = NULL;
	unsigned int flags = NO_FLAGS;
	resource_resv *job;
	status *policy;
	server_info *sinfo;
	schd_error *err = NULL;

	if (thread_data == NULL)
		return NULL;

	thread_data->job_seen = 1;
	job = thread_data->job;
	policy = thread_data->policy;
	sinfo = thread_data->sinfo;
	err = thread_data->err;

	if (job == NULL)
		return NULL;

	qinfo = job->job->queue;

	err->status_code = NOT_RUN;

	if(thread_data->should_use_buckets)
		flags = USE_BUCKETS;

	if (job->is_shrink_to_fit) {
		/* Pass the suitable heuristic for shrinking */
		ns_arr = is_ok_to_run_STF(policy, sinfo, qinfo, job, flags, err, shrink_job_algorithm);
	} else
		ns_arr = is_ok_to_run(policy, sinfo, qinfo, job, flags, err);

	if (err->status_code == NEVER_RUN)
		job->can_never_run = 1;

	if (ns_arr != NULL) {	/* The job can run! */
		thread_data->ns_arr = ns_arr;
	} else if (policy->preempting && in_runnable_state(job) && (!job->can_never_run)) {
		thread_data->pjob_ranks = find_jobs_to_preempt(policy, job, sinfo, NULL);
	}

	return ns_arr;
}

/*
 * @brief	Constructor for  checkjob_thread_data
 *
 * @param	job - the next job being evaluated
 * @param	policy - policy info
 * @param	sinfo - server info
 *
 * @return checkjob_thread_data *
 * @retval pointer to newly allocated checkjob_thread_data object
 * @retval NULL for malloc error
 */
checkjob_thread_data *
create_checkjob_thread_data(resource_resv *job, status *policy, server_info *sinfo)
{
	checkjob_thread_data *new_obj = NULL;

	new_obj = malloc(sizeof(checkjob_thread_data));
	if (new_obj == NULL) {
		snprintf(log_buffer, sizeof(log_buffer), "Error malloc'ing thread memory");
		schdlog(PBSEVENT_ERROR, PBS_EVENTCLASS_SCHED, LOG_ERR, __func__, log_buffer);
		return NULL;
	}

	new_obj->job_seen = 0;
	new_obj->job = job;
	new_obj->policy = policy;
	new_obj->sinfo = sinfo;
	new_obj->ns_arr = NULL;
	new_obj->pjob_ranks = NULL;
	new_obj->should_use_buckets = job_should_use_buckets(job);
	new_obj->err = new_schd_error();
	if (new_obj->err == NULL) {
		free(new_obj);
		snprintf(log_buffer, sizeof(log_buffer), "Error malloc'ing thread memory");
		schdlog(PBSEVENT_ERROR, PBS_EVENTCLASS_SCHED, LOG_ERR, __func__, log_buffer);
		return NULL;
	}

	/*
	 * Save the order info for this job,
	 * this will be used to restore order info for next call to find_run_a_job
	 */
	new_obj->jorder_info = order_info;

	return new_obj;
}

/**
 * @brief	Destructor for checkjob_thread_data
 *
 * @param[in,out]	obj - the object to deallocate
 *
 * @return void
 */
void
free_check_thread_data(checkjob_thread_data *obj)
{
	if (obj == NULL)
		return;

	free_schd_error(obj->err);
	free_nspecs(obj->ns_arr);
	free(obj->pjob_ranks);
	free(obj);
}

/**
 * @brief	Destructor for checkjob_thread_data
 *
 * @param[in,out]	list - the list to be destroyed
 *
 * @return void
 */
void
free_checkjob_thread_data_list(checkjob_thread_data **list)
{
	int i;

	for (i = 0; list[i] != NULL; i++) {
		free_check_thread_data(list[i]);
	}

	free(list);
}
