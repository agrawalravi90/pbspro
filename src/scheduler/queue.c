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

#include "queue.h"

#include "pbs_error.h"


/**
 * @brief	Constructor for the data structure 'queue'
 *
 * @param	void
 *
 * @return queue *
 * @retval a newly allocated queue object
 * @retval NULL for malloc error
 */
schd_queue *
new_schd_queue(void)
{
	schd_queue *ret_obj = NULL;

	ret_obj = malloc(sizeof(schd_queue));
	if (ret_obj == NULL) {
		pbs_errno = PBSE_SYSTEM;
		return NULL;
	}

	ret_obj->min_size = QUEUE_DS_MIN_SIZE;
	ret_obj->content_arr = NULL;
	ret_obj->front = 0;
	ret_obj->rear = 0;
	ret_obj->q_size = 0;

	return ret_obj;
}

/**
 * @brief	Destructor for a queue object
 *
 * @param[in]	obj - a queue object to deallocate
 *
 * @return void
 */
void
delete_schd_queue(schd_queue *obj)
{
	free(obj->content_arr);
	free(obj);
}


/**
 * @brief	Enqueue an object into the queue
 *
 * @param[in]	queue - the queue to enqueue the object in
 * @param[in]	obj - the object to enqueue
 *
 * @return int
 * @retval 0 for Success
 * @retval 1 for Failure
 */
int
schd_enqueue(schd_queue *queue, void *obj)
{
	long curr_rear;
	long curr_qsize;

	if (queue == NULL || obj == NULL) {
		pbs_errno = PBSE_INTERNAL;
		return 1;
	}

	curr_rear = queue->rear;
	curr_qsize = queue->q_size;

	if (curr_rear >= curr_qsize) {
		long new_qsize;
		void **realloc_ptr = NULL;

		/* Need to resize the queue */
		if (curr_qsize == 0) /* First enqueue operation */
			new_qsize = queue->min_size;
		else
			new_qsize = 2 * curr_qsize;

		realloc_ptr = (void **) realloc(queue->content_arr, new_qsize * sizeof(void *));
		if (realloc_ptr == NULL) {
			pbs_errno = PBSE_SYSTEM;
			return 1;
		}

		queue->content_arr = realloc_ptr;
		queue->q_size = new_qsize;
	}

	queue->content_arr[curr_rear] = obj;
	queue->rear = curr_rear + 1;

	return 0;
}

/**
 * @brief	Dequeue an object from the queue
 *
 * @param[in]	queue - the queue to use
 *
 * @return void *
 * @retval the first item in queue
 * @retval NULL for error/empty queue
 */
void *
schd_dequeue(schd_queue *queue)
{
	if (queue == NULL)
		return NULL;

	if (queue->front == queue->rear) /* queue is empty */
		return NULL;

	return queue->content_arr[queue->front++];
}

/**
 * @brief	Check if a queue is empty
 *
 * @param[in]	queue  - the queue to use
 *
 * @return int
 * @retval 1 if queue is empty
 * @retval 0 otherwise
 */
int
schd_is_empty(schd_queue *queue)
{
	if (queue == NULL || queue->front == queue->rear)
		return 1;
	else
		return 0;
}
