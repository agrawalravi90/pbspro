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
 * @file	pbs_runjob.c
 */

#include <pbs_config.h>   /* the master config generated by configure */

#include <string.h>
#include <stdio.h>
#include "libpbs.h"
#include "dis.h"
#include "pbs_ecl.h"


/**
 * @brief	Helper function for pbs_asynrunjob and pbs_asynrunjob_ack
 *
 * @param[in] c - connection handle
 * @param[in] jobid- job identifier
 * @param[in] location - string of vnodes/resources to be allocated to the job
 * @param[in] extend - extend string for encoding req
 * @param[in] req_type - one of PBS_BATCH_AsyrunJob or PBS_BATCH_AsyrunJob_ack
 *
 * @return      int
 * @retval      0       success
 * @retval      !0      error
 */
static int
__runjob_helper(int c, char *jobid, char *location, char *extend, int req_type)
{
	int	rc = 0;
	unsigned long resch = 0;

	if ((jobid == NULL) || (*jobid == '\0'))
		return (pbs_errno = PBSE_IVALREQ);
	if (location == NULL)
		location = "";

	/* initialize the thread context data, if not already initialized */
	if (pbs_client_thread_init_thread_context() != 0)
		return pbs_errno;

	/* lock pthread mutex here for this connection */
	/* blocking call, waits for mutex release */
	if (pbs_client_thread_lock_connection(c) != 0)
		return pbs_errno;

	/* setup DIS support routines for following DIS calls */

	DIS_tcp_funcs();

	/* send run request */

	if ((rc = encode_DIS_ReqHdr(c, req_type, pbs_current_user)) ||
		(rc = encode_DIS_Run(c, jobid, location, resch)) ||
		(rc = encode_DIS_ReqExtend(c, extend))) {
		if (set_conn_errtxt(c, dis_emsg[rc]) != 0) {
			pbs_errno = PBSE_SYSTEM;
		} else {
			pbs_errno = PBSE_PROTOCOL;
		}
		(void)pbs_client_thread_unlock_connection(c);
		return pbs_errno;
	}

	if (dis_flush(c)) {
		pbs_errno = PBSE_PROTOCOL;
		(void)pbs_client_thread_unlock_connection(c);
		return pbs_errno;
	}

	if (req_type == PBS_BATCH_AsyrunJob_ack) {
		struct batch_reply *reply = NULL;

		/* Get reply */
		reply = PBSD_rdrpy(c);
		rc = get_conn_errno(c);
		PBSD_FreeReply(reply);
	}

	/* unlock the thread lock and update the thread context data */
	if (pbs_client_thread_unlock_connection(c) != 0)
		return pbs_errno;

	return rc;
}

/**
 * @brief
 *	-send async run job batch request.
 *
 * @param[in] c - connection handle
 * @param[in] jobid- job identifier
 * @param[in] location - string of vnodes/resources to be allocated to the job
 * @param[in] extend - extend string for encoding req
 *
 * @return      int
 * @retval      0       success
 * @retval      !0      error
 *
 */
int
__pbs_asyrunjob(int c, char *jobid, char *location, char *extend)
{
	return __runjob_helper(c, jobid, location, extend, PBS_BATCH_AsyrunJob);
}

/**
 * @brief
 *	-send a run job batch request which waits for an ack from server
 *	pbs_runjob() and pbs_asyrunjob_ack() are similar in the fact that they both wait for an ack back from the server,
 *	but this call is faster than pbs_rubjob() because the server returns before contacting MoM
 *
 * @param[in] c - connection handle
 * @param[in] jobid- job identifier
 * @param[in] location - string of vnodes/resources to be allocated to the job
 * @param[in] extend - extend string for encoding req
 *
 * @return      int
 * @retval      0       success
 * @retval      !0      error
 *
 */
int
pbs_asyrunjob_ack(int c, char *jobid, char *location, char *extend)
{
	return __runjob_helper(c, jobid, location, extend, PBS_BATCH_AsyrunJob_ack);
}
