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

/** @file	int_status.c
 * @brief
 * The function that underlies all the status requests
 */

#include <pbs_config.h>   /* the master config generated by configure */

#include <string.h>
#include <stdio.h>
#include "libpbs.h"
#include "pbs_ecl.h"
#include "libutil.h"


static struct batch_status *alloc_bs();

/**
 * @brief
 *	get one of the available connection from multisvr sd
 *
 */
int
get_available_conn(svr_conn_t *svr_connections)
{
	int i;

	for (i = 0; i < get_num_servers(); i++)
		if (svr_connections[i].state == SVR_CONN_STATE_CONNECTED)
			return svr_connections[i].sd;

	return -1;
}

/**
 * @brief
 *	get random server sd - It will choose a random sd from available no of servers.
 *
 */
int
random_srv_conn(svr_conn_t *svr_connections)
{
	int ind = 0;

	srand(time(0));
	ind =  rand() % get_num_servers();

	if (svr_connections[ind].state == SVR_CONN_STATE_CONNECTED)
		return svr_connections[ind].sd;
		
	return get_available_conn(svr_connections);
}

/**
 * @brief
 *	-wrapper function for PBSD_status_put which sends
 *	status batch request
 *
 * @param[in] c - socket descriptor
 * @param[in] function - request type
 * @param[in] id - object id
 * @param[in] attrib - pointer to attribute list
 * @param[in] extend - extention string for req encode
 *
 * @return	structure handle
 * @retval 	pointer to batch status on SUCCESS
 * @retval 	NULL on failure
 *
 */
struct batch_status *
PBSD_status(int c, int function, char *objid, struct attrl *attrib, char *extend)
{
	int rc;
	struct batch_status *PBSD_status_get(int c);

	/* send the status request */

	if (objid == NULL)
		objid = "";	/* set to null string for encoding */

	rc = PBSD_status_put(c, function, objid, attrib, extend, PROT_TCP, NULL);
	if (rc) {
		return NULL;
	}

	/* get the status reply */
	return (PBSD_status_get(c));
}

/**
 * @brief
 *	wrapper function for PBSD_status
 *	gets aggregated value for all servers.
 *
 * @param[in] c - communication handle
 * @param[in] id - job id
 * @param[in] attrib - pointer to attribute list
 * @param[in] extend - extend string for req
 * @param[in] cmd - command
 *
 * @return	structure handle
 * @retval	pointer to batch_status struct		success
 * @retval	NULL					error
 *
 */
struct batch_status *
PBSD_status_aggregate(int c, int cmd, char *id, struct attrl *attrib, char *extend, int parent_object)
{
	int i;
	struct batch_status *ret = NULL;
	struct batch_status *next = NULL;
	struct batch_status *cur = NULL;
	svr_conn_t *svr_connections = get_conn_servers(1);

	if (!svr_connections)
		return NULL;

	/* initialize the thread context data, if not already initialized */
	if (pbs_client_thread_init_thread_context() != 0)
		return NULL;

	/* first verify the attributes, if verification is enabled */
	if ((pbs_verify_attributes(random_srv_conn(svr_connections), cmd,
		parent_object, MGR_CMD_NONE, (struct attropl *) attrib)))
		return NULL;

	for (i = 0; i < get_num_servers(); i++) {
		if (svr_connections[i].state != SVR_CONN_STATE_CONNECTED)
			continue;

		c = svr_connections[i].sd;

		if (pbs_client_thread_lock_connection(c) != 0)
			return NULL;

		if ((next = PBSD_status(c, cmd, id, attrib, extend))) {
			if (!ret) {
				ret = next;
				cur = next->last;
			} else {
				cur->next = next;
				cur = next->last;
			}
		}

		/* unlock the thread lock and update the thread context data */
		if (pbs_client_thread_unlock_connection(c) != 0)
			return NULL;
	}

	return ret;
}

/**
 * @brief
 *	wrapper function for PBSD_status
 *	gets status randomly from one of the configured server.
 *
 * @param[in] c - communication handle
 * @param[in] id - job id
 * @param[in] attrib - pointer to attribute list
 * @param[in] extend - extend string for req
 * @param[in] cmd - command
 *
 * @return	structure handle
 * @retval	pointer to batch_status struct		success
 * @retval	NULL					error
 *
 */
struct batch_status *
PBSD_status_random(int c, int cmd, char *id, struct attrl *attrib, char *extend, int parent_object)
{
	struct batch_status *ret = NULL;
	svr_conn_t *svr_connections = get_conn_servers(1);

	if (!svr_connections)
		return NULL;

	if ((c = random_srv_conn(svr_connections)) < 0)
		return NULL;

	/* initialize the thread context data, if not already initialized */
	if (pbs_client_thread_init_thread_context() != 0)
		return NULL;

	/* first verify the attributes, if verification is enabled */
	if ((pbs_verify_attributes(c, cmd, parent_object, MGR_CMD_NONE, (struct attropl *) attrib)))
		return NULL;

	if (pbs_client_thread_lock_connection(c) != 0)
		return NULL;

	ret = PBSD_status(c, cmd, id, attrib, extend);

	/* unlock the thread lock and update the thread context data */
	if (pbs_client_thread_unlock_connection(c) != 0)
		return NULL;

	return ret;
}

/**
 * @brief
 *	Returns pointer to status record
 *
 * @param[in] c - index into connection table
 *
 * @return returns a pointer to a batch_status structure
 * @retval pointer to batch status on SUCCESS
 * @retval NULL on failure
 */
struct batch_status *
PBSD_status_get(int c)
{
	struct brp_cmdstat  *stp; /* pointer to a returned status record */
	struct batch_status *bsp  = NULL;
	struct batch_status *rbsp = NULL;
	struct batch_reply  *reply;
	int i;

	/* read reply from stream into presentation element */

	reply = PBSD_rdrpy(c);
	if (reply == NULL) {
		pbs_errno = PBSE_PROTOCOL;
	} else if (reply->brp_choice != BATCH_REPLY_CHOICE_NULL  &&
		reply->brp_choice != BATCH_REPLY_CHOICE_Text &&
		reply->brp_choice != BATCH_REPLY_CHOICE_Status) {
		pbs_errno = PBSE_PROTOCOL;
	} else if (get_conn_errno(c) == 0) {
		/* have zero or more attrl structs to decode here */
		stp = reply->brp_un.brp_statc;
		i = 0;
		pbs_errno = 0;
		while (stp != NULL) {
			if (i++ == 0) {
				rbsp = bsp = alloc_bs();
				if (bsp == NULL) {
					pbs_errno = PBSE_SYSTEM;
					break;
				}
			} else {
				bsp->next = alloc_bs();
				bsp = bsp->next;
				if (bsp == NULL) {
					pbs_errno = PBSE_SYSTEM;
					break;
				}
			}
			if ((bsp->name = strdup(stp->brp_objname)) == NULL) {
				pbs_errno = PBSE_SYSTEM;
				break;
			}
			bsp->attribs = stp->brp_attrl;
			if (stp->brp_attrl)
				stp->brp_attrl = 0;
			bsp->next = NULL;
			rbsp->last = bsp;
			stp = stp->brp_stlink;
		}
		if (pbs_errno) {
			pbs_statfree(rbsp);
			rbsp = NULL;
		}
	}
	PBSD_FreeReply(reply);
	return rbsp;
}

/**
 * @brief
 *	Allocate a batch status reply structure
 */
static struct batch_status *
alloc_bs()
{
	struct batch_status *bsp;

	bsp = (struct batch_status *)malloc(sizeof(struct batch_status));
	if (bsp) {

		bsp->next = NULL;
		bsp->name = NULL;
		bsp->attribs = NULL;
		bsp->text = NULL;
	}
	return bsp;
}
