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

#include <pbs_config.h>

#include <stdio.h>
#include <stdlib.h>
#include <errno.h>

#include "log.h"
#include "data_types.h"
#include "constant.h"

/**
 * @brief	Constructor for dyn_arr structure
 *
 * @param[in]	inarr - input array to use to create the dynamic array (optional)
 *
 * @return dyn_arr *
 * @retval newly allocated dyn_arr structure
 * @retval NULL on malloc error
 */
dyn_arr *
new_dyn_arr(void **inarr)
{
	dyn_arr *ret_arr = NULL;

	ret_arr = malloc(sizeof(dyn_arr));
	if (ret_arr == NULL) {
		log_err(errno, __func__, MEM_ERR_MSG);
		return NULL;
	}

	if (inarr != NULL) { /* copy constructor */
		long i;
		long num_in;

		for (i = 0; inarr[i] != NULL; i++)
			;

		num_in = i;
		ret_arr->arr = inarr;
		ret_arr->size = num_in + 1; /* 1 extra for the NULL slot */
		ret_arr->num_items = num_in;
	} else { /* default constructor */
		ret_arr->arr = malloc(MIN_DYN_ARR_SIZE * sizeof(void *));
		if (ret_arr->arr == NULL) {
			free(ret_arr);
			log_err(errno, __func__, MEM_ERR_MSG);
			return NULL;
		}
		ret_arr->size = MIN_DYN_ARR_SIZE;
		ret_arr->arr[0] = NULL;
		ret_arr->num_items = 0;
	}

	return ret_arr;
}


/**
 * @brief	Destructor for dyn_res struct
 *
 * @param[out]	darr - the object to deallocate
 *
 * @return void
 */
void
free_dyn_arr(dyn_arr *darr)
{
	if (darr == NULL)
		return;

	free(darr->arr);
	free(darr);
}

/**
 * @brief	Function to insert an element to a dynamic array
 *
 * @param[in,out]	darr - the array to insert into
 * @param[in]		item - the item to insert
 *
 * @return int
 * @retval 1 for Success
 * @retval 0 for error
 */
int
dyn_arr_insert(dyn_arr *darr, void *item)
{
	long new_nitems;

	if (darr == NULL || item == NULL)
		return 0;

	new_nitems = darr->num_items + 1;
	if (new_nitems >= darr->size) {	/* Need to realloc */
		long new_size;
		void **realloc_ptr = NULL;

		new_size = darr->size * 2;
		realloc_ptr = realloc(darr->arr, new_size * sizeof(void *));
		if (realloc_ptr == NULL) {
			log_err(errno, __func__, MEM_ERR_MSG);
			return 0;
		}
		darr->arr = realloc_ptr;
		darr->size = new_size;
	}
	darr->arr[new_nitems - 1] = item;
	darr->arr[new_nitems] = NULL;
	darr->num_items = new_nitems;

	return 1;
}

/**
 * @brief	Delete an item from the dynamic array given
 *
 * @param[in,out]	darr - pointer to the dynamic array container
 * @param[in]	item - the item to delete
 *
 * @return int
 * @retval 1 for Success
 * @retval 0 for Failure
 */
int
dyn_arr_delete(dyn_arr *darr, void *item)
{
	int i;

	if (darr == NULL || item == NULL)
		return 0;

	for (i = 0; darr->arr[i] != NULL && darr->arr[i] != item; i++)
		;

	if (darr->arr[i] == item) {
		/* copy all the jobs past the one we found back one spot.  Including
		 * coping the NULL back one as well
		 */
		for (; darr->arr[i] != NULL; i++)
			darr->arr[i] =  darr->arr[i+1];
	}
	darr->num_items--;

	return 1;
}


