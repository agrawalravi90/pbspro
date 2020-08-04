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

/**
 * @file	disrull.c
 *
 * @par Synopsis:
 *	u_Long disrull(int stream, int *retval)
 *
 *	Gets a Data-is-Strings unsigned integer from <stream>, converts it into
 *	an u_Long, and returns it.
 *
 *	This format for character strings representing unsigned integers can
 *	best be understood through the decoding algorithm:
 *
 *	1. Initialize the digit count to 1.
 *
 *	2. Read the next character; if it is a plus sign, go to step (4); if it
 *	   is a minus sign, post an error.
 *
 *	3. Decode a new count from the digit decoded in step (2) and the next
 *	   count - 1 digits; repeat step (2).
 *
 *	4. Decode the next count digits as the unsigned integer.
 *
 *	*<retval> gets DIS_SUCCESS if everything works well.  It gets an error
 *	code otherwise.  In case of an error, the <stream> character pointer is
 *	reset, making it possible to retry with some other conversion strategy.
 */

#include <pbs_config.h>   /* the master config generated by configure */

#include <assert.h>
#include <stddef.h>

#include "dis.h"
#include "dis_.h"

/**
 * @brief
 *  	Function to read a string which comes over network from
 *  	mom to server and convert that string data to numeric form of type
 *  	u_Long
 *
 * @param[in] stream - pointer to data stream
 * @param[out] retval - return value
 *
 * @return	u_Long
 * @retval	converted value		success
 * @retval	0			error
 *
 */


u_Long
disrull(int stream, int *retval)
{
	int		locret;
	int		negate;
	u_Long		value;

	assert(retval != NULL);

	locret = disrsll_(stream, &negate, &value, 1, 0);
	if (locret != DIS_SUCCESS) {
		value = 0;
	} else if (negate) {
		value = 0;
		locret = DIS_BADSIGN;
	}
	*retval = locret;
	return (value);
}
