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
#include <ctype.h>
#include "libpbs.h"
#include "pbs_ecl.h"
#include "libutil.h"
#include "attribute.h"
#include "cmds.h"
#include "pbs_internal.h"


extern char * PBS_get_server(char *, char *, uint *);


/**
 * @brief
 * 	get_available_conn:
 *	get one of the available connection from multisvr server list
 *
 * @param[in] svr_connections - pointer to array of server connections
 * 
 * @return int
 * @retval -1: error
 * @retval != -1 fd corresponding to the connection
 */
int
get_available_conn(svr_conn_t *svr_connections)
{
	int i;

	for (i = 0; i < get_num_servers(); i++)
		if (svr_connections[i].state == SVR_CONN_STATE_UP)
			return svr_connections[i].sd;

	return -1;
}

/**
 * @brief
 * random_srv_conn:
 *	get random server sd - It will choose a random sd from available no of servers.
 * @param[in] svr_connections - pointer to array of server connections
 * 
 * @return int
 * @retval -1: error
 * @retval != -1: fd corresponding to the connection
 */
int
random_srv_conn(svr_conn_t *svr_connections)
{
	int ind = 0;

	ind =  rand_num() % get_num_servers();

	if (svr_connections[ind].state == SVR_CONN_STATE_UP)
		return svr_connections[ind].sd;
		
	return get_available_conn(svr_connections);
}

enum state { TRANSIT_STATE,
	     QUEUE_STATE,
	     HELD_STATE,
	     WAIT_STATE,
	     RUN_STATE,
	     EXITING_STATE,
	     BEGUN_STATE,
	     MAX_STATE };

static char *statename[] = { "Transit", "Queued", "Held", "Waiting",
		"Running", "Exiting", "Begun"};

/**
 * @brief
 *	decode_states - decoded state attribute to count array
 *
 * @param[in] string - string holding state of job
 * @param[out] count    - count array having job per state
 * 
 * @return void
 *
 */
static void
decode_states(char *string, long *count)
{
	char *c;
	int i;
	int rc;
	static char *format_str = NULL;

	c = string;
	while (isspace(*c) && *c != '\0')
		c++;

	if (!format_str) {
		format_str = malloc(12 * MAX_STATE); /* 12 = max char in state name + 4 on type specifier */
		int len = 0;
		for (i = 0; i < MAX_STATE; i++)
			len += sprintf(format_str + len, "%s:%%ld ", statename[i]);
	}

	rc = sscanf(c, format_str, &count[TRANSIT_STATE], &count[QUEUE_STATE],
	       &count[HELD_STATE], &count[WAIT_STATE], &count[RUN_STATE],
	       &count[EXITING_STATE], &count[BEGUN_STATE]);

	if (rc != MAX_STATE)
		pbs_errno = PBSE_BADATVAL;
}

/**
 * @brief
 *	encode_states - Encode state from two state count arrays
 *
 * @param[in,out] val - reference to pointer which will get freed and new value gets allocated
 * @param[in] cur    - count array having job per state
 * @param[in] next    - count array having job per state
 * 
 * @return void
 *
 */
static void
encode_states(char **val, long *cur, long *nxt)
{
	int index;
	int len = 0;
	char buf[256];

	buf[0] = '\0';
	for (index = 0; index < MAX_STATE; index++) {
		len += sprintf(buf + len, "%s:%ld ", statename[index],
			       cur[index] + nxt[index]);
	}
	free(*val);

	if ((*val = strdup(buf)) == NULL)
		pbs_errno = PBSE_SYSTEM;
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

	/* send the status request */

	if (objid == NULL)
		objid = "";	/* set to null string for encoding */

	rc = PBSD_status_put(c, function, objid, attrib, extend, PROT_TCP, NULL);
	if (rc) {
		return NULL;
	}

	/* get the status reply */
	return (PBSD_status_get(c, NULL));
}

/**
 * @brief
 * aggr_job_ct:
 *	aggregate the job counts attribute of servers
 *	total_jobs and state_counts are the attributes aggregated here.
 * @param[in] cur - batch status to first server
 * @param[in] next - batch status to second server
 * 
 * @return void
 */
static void
aggr_job_ct(struct batch_status *cur, struct batch_status *nxt)
{
	long cur_st_ct[MAX_STATE] = {0};
	long nxt_st_ct[MAX_STATE] = {0};
	struct attrl *a = NULL;
	struct attrl *b = NULL;
	char *tot_jobs_attr = NULL;
	long tot_jobs = 0;
	char *endp;
	int found;
	char **orig_st_ct = NULL;

	if (!cur || !nxt)
		return;

	for (a = cur->attribs, found = 0; a; a = a->next) {
		if (a->name && strcmp(a->name, ATTR_count) == 0) {
			decode_states(a->value, cur_st_ct);
			orig_st_ct = &a->value;
			found++;
		} else if (a->name && strcmp(a->name, ATTR_total) == 0) {
			tot_jobs_attr = a->value;
			tot_jobs += strtol(a->value, &endp, 10);
			found++;
		}
		if (found == 2)
			break;
	}
	for (b = nxt->attribs, found = 0; b; b = b->next) {
		if (b->name && strcmp(b->name, ATTR_count) == 0) {
			decode_states(b->value, nxt_st_ct);
			found++;
		} else if (b->name && strcmp(b->name, ATTR_total) == 0) {
			tot_jobs += strtol(b->value, &endp, 10);
			found++;
		}
		if (found == 2)
			break;
	}

	if (orig_st_ct)
		encode_states(orig_st_ct, cur_st_ct, nxt_st_ct);
	if (tot_jobs_attr)
		sprintf(tot_jobs_attr, "%ld", tot_jobs);
}

/**
 * @brief
 * assess_type:
 *	Assess the type of the value and return the value along with
 *	the references passed to it.
 * @param[in] val - value for which type needs to be assessed
 * @param[out] type - The type of val: can be long, double, size or string.
 * @param[out] val_double - double value in case if the type is double or floating number.
 * @param[out] val_long - long in case if the type is long or size
 * 
 * @return void
 */
static void
assess_type(char *val, int *type, double *val_double, long *val_long)
{
	char *pc;

	if (strchr(val, '.')) {
		if ((*val_double = strtod(val, &pc)))
			*type = ATR_TYPE_FLOAT;
		else if (pc && *pc != '\0')
			*type = ATR_TYPE_STR;
	} else {
		*val_long = strtol(val, &pc, 10);
		if (!pc || *pc == '\0')
			*type = ATR_TYPE_LONG;
		else if (pc && (!strcasecmp(pc, "kb") || !strcasecmp(pc, "mb") || !strcasecmp(pc, "gb") ||
			    !strcasecmp(pc, "tb") || !strcasecmp(pc, "pb")))
				*type = ATR_TYPE_SIZE;
		else
			*type = ATR_TYPE_STR;
	}
}

struct attrl_holder {
	struct attrl *atr_list;
	struct attrl_holder *next;
};

/**
 * @brief
 * accumulate_values:
 *	Accumulate values in resources assigned attribute in b
 * @param[in] a - input list of type attrl_holder. 
 * 		It will contain only resources assigned those needs to be added with the corresponding in b.
 * @param[in] b - attribute list which needs to be added with a. This function does not iterate through the list.
 * @param[in,out] orig - original list which is the superset of a. b will be appended to orig if could not find in a.
 * 
 * @return void
 */
static void
accumulate_values(struct attrl_holder *a, struct attrl *b, struct attrl *orig)
{
	double val_double = 0;
	long val_long = 0;
	char *pc;
	int type = -1;
	struct attrl_holder *itr;
	struct attribute attr;
	struct attribute new;
	struct attrl *cur;
	char buf[32];

	if (!b || !b->resource || *b->resource == '\0' || !b->value || *b->value == '\0')
		return;

	assess_type(b->value, &type, &val_double, &val_long);

	if (type == ATR_TYPE_STR)
		return;

	for (itr = a; itr && itr->atr_list; itr = itr->next) {
		cur = itr->atr_list;
		if (cur->resource && !strcmp(cur->resource, b->resource)) {
			switch (type) {
			case ATR_TYPE_FLOAT:
				val_double += strtod(cur->value, &pc);
				sprintf(buf, "%f", val_double);
				break;
			case ATR_TYPE_LONG:
				val_long += strtol(cur->value, &pc, 10);
				sprintf(buf, "%ld", val_long);
				break;
			case ATR_TYPE_SIZE:
				decode_size(&attr, NULL, NULL, b->value);
				decode_size(&new, NULL, NULL, cur->value);
				set_size(&attr, &new, INCR);
				from_size(&attr.at_val.at_size, buf);
			default:
				break;
			}

			free(cur->value);
			if ((cur->value = strdup(buf)) == NULL)
				pbs_errno = PBSE_SYSTEM;
			break;
		}
	}

	/* value exists in next but not in cur. Create it */
	if (!itr) {
		struct attrl *at;
		if ((at = dup_attrl(b)) == NULL) {
			pbs_errno = PBSE_SYSTEM;
			return;
		}
		for (cur = orig; cur->next; cur = cur->next)
			;
		cur->next = at;
	}
}

/**
 * @brief
 * aggr_resc_ct:
 *	Aggregate all resources assigned attributes from two batch status.
 * @param[in] st1 - input list of type attrl_holder. 
 * 		It will contain only resources assigned those needs to be added with the corresponding in b.
 * @param[in] b - attribute list which needs to be added with a. This function does not iterate through the list.
 * @param[in,out] orig - original list which is the superset of a. b will be appended to orig if could not find in a.
 * 
 * @return void
 */
static void
aggr_resc_ct(struct batch_status *st1, struct batch_status *st2)
{
	struct attrl *a = NULL;
	struct attrl *b = NULL;
	struct attrl_holder *resc_assn = NULL;
	struct attrl_holder *cur = NULL;
	struct attrl_holder *nxt = NULL;

	if (!st1 || !st2)
		return;

	/* In the first pass gather all resources assigned attr from st1
		so we do not have to loop through all attributes */
	for (a = st1->attribs; a; a = a->next) {
		if (a->name && strcmp(a->name, ATTR_rescassn) == 0) {
			if ((nxt = malloc(sizeof(struct attrl_holder))) == NULL) {
				pbs_errno = PBSE_SYSTEM;
				goto end;
			}
			nxt->atr_list = a;
			nxt->next = NULL;
			if (cur) {
				cur->next = nxt;
				cur = cur->next;
			} else
				resc_assn = cur = nxt;
		}
	}

	for (b = st2->attribs; b; b = b->next) {
		if (b->name && strcmp(b->name, ATTR_rescassn) == 0)
			accumulate_values(resc_assn, b, st1->attribs);
	}

end:
	for (cur = resc_assn; cur; cur = nxt) {
		nxt = cur->next;
		free(cur);
	}
}

/**
 * @brief
 * append_bs:
 *	append b to end of a. also remove references of b from its batch status
 * @param[in,out] a - attr list where b needs to be appended
 * @param[in] b - batch status which needs to be appended to a
 * @param[in,out] prev_b - previous list element of b for which references needs to be updated
 * @param[in] head_a - head element of list contains a
 * @param[in,out] head_b - reference to head element of list contains b. Reference is updated if prev_b is null
 * 
 * @return void
 */
static void
append_bs(struct batch_status *a, struct batch_status *b, struct batch_status *prev_b, struct batch_status *head_a, struct batch_status **head_b)
{
	if (!a) {
		for (a = head_a; a->next; a = a->next)
			;
		a->next = b;
		if (prev_b) {
			prev_b->next = b->next;
		} else {
			*head_b = b->next;
		}
		b->next = NULL;
	}
}

/**
 * @brief
 * aggregate_queue:
 *	aggregate two queues reported by two server instances
 * @param[in,out] sv1 - server1 list. This will be also the final aggregated list.
 * @param[in,out] sv2 - Server2 list. Passing reference as the first element can be moved to sv1
 * 
 * @return void
 */
static void
aggregate_queue(struct batch_status *sv1, struct batch_status **sv2)
{
	struct batch_status *a = NULL;
	struct batch_status *b = NULL;
	struct batch_status *prev_b = NULL;

	for (b = *sv2; b; prev_b = b, b = b->next) {
		for (a = sv1; a; a = a->next) {
			if (a->name && b->name && !strcmp(a->name, b->name)) {
				aggr_job_ct(a, b);
				aggr_resc_ct(a, b);
				break;
			}
		}

		if (!a)
			append_bs(a, b, prev_b, sv1, sv2);
	}
}

/**
 * @brief
 * aggregate_svr:
 *	aggregate two servers reported by two server instances
 * @param[in,out] sv1 - server1 list. This will be also the final aggregated list.
 * @param[in] sv2 - Server2 list.
 * 
 * @return void
 */
static void
aggregate_svr(struct batch_status *sv1, struct batch_status *sv2)
{
	aggr_job_ct(sv1, sv2);
	aggr_resc_ct(sv1, sv2);
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
PBSD_status_aggregate(int c, int cmd, char *id, void *attrib, char *extend, int parent_object, struct attrl *rattrib)
{
	int i;
	int rc = 0;
	struct batch_status *ret = NULL;
	struct batch_status *next = NULL;
	struct batch_status *cur = NULL;
	svr_conn_t *svr_connections = get_conn_svr_instances(c);
	int num_cfg_svrs = get_num_servers();
	int *failed_conn;
	char server_out[PBS_MAXSERVERNAME + 1];
	char server_name[PBS_MAXSERVERNAME + 1];
	int single_itr = 0;
	char job_id_out[PBS_MAXCLTJOBID];
	uint server_port;
	int start = 0;
	int ct;
	struct batch_status *last = NULL;

	if (!svr_connections)
		return NULL;

	failed_conn = calloc(num_cfg_svrs, sizeof(int));

	if (pbs_client_thread_init_thread_context() != 0)
		return NULL;

	if (pbs_verify_attributes(random_srv_conn(svr_connections), cmd, parent_object, MGR_CMD_NONE, (struct attropl *) attrib) != 0)
		return NULL;

	if ((parent_object == MGR_OBJ_JOB) && (get_server(id, job_id_out, server_out) == 0)) {
		if (PBS_get_server(server_out, server_name, &server_port)) {
			single_itr = 1;
			for (i = 0; i < num_cfg_svrs; i++) {
				if (!strcmp(server_name, pbs_conf.psi[i].name) && (server_port == pbs_conf.psi[i].port)) {
					start = i;
					break;
				}
			}
		}
	}

	if (pbs_client_thread_lock_connection(c) != 0)
		return NULL;

	for (i = start, ct = 0; ct < num_cfg_svrs; i = (i + 1) % num_cfg_svrs, ct++) {
		if (svr_connections[i].state != SVR_CONN_STATE_UP) {
			rc = PBSE_NOSERVER;
			continue;
		}

		if (cmd == PBS_BATCH_SelStat) {
			rc = PBSD_select_put(svr_connections[i].sd, PBS_BATCH_SelStat, (struct attropl *) attrib, rattrib, extend);
		} else {
			if (id == NULL)
				id = "";

			rc = PBSD_status_put(svr_connections[i].sd, cmd, id, (struct attrl *) attrib, extend, PROT_TCP, NULL);
		}

		if (rc) {
			failed_conn[i] = 1;
			continue;
		} else if (single_itr)
			break;
	}

	for (i = start, ct = 0; ct < num_cfg_svrs; i = (i + 1) % num_cfg_svrs, ct++) {

		if (svr_connections[i].state != SVR_CONN_STATE_UP || failed_conn[i])
			continue;

		if ((next = PBSD_status_get(svr_connections[i].sd, &last))) {
			if (!ret) {
				ret = next;
				cur = last;
			} else {
				switch (parent_object) {
				case MGR_OBJ_SERVER:
					aggregate_svr(ret, next);
					pbs_statfree(next);
					next = NULL;
					break;
				case MGR_OBJ_QUEUE:
					aggregate_queue(ret, &next);
					pbs_statfree(next);
					next = NULL;
					break;
				default:
					cur->next = next;
					cur = last;
				}
			}
		}

		if (single_itr)
			break;
	}

	/* unlock the thread lock and update the thread context data */
	if (pbs_client_thread_unlock_connection(c) != 0)
		return NULL;

	if (rc)
		pbs_errno = rc;

	free(failed_conn);
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
	svr_conn_t *svr_connections = get_conn_svr_instances(c);

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
PBSD_status_get(int c, struct batch_status **last)
{
	struct batch_status *rbsp = NULL;
	struct batch_reply  *reply;

	/* read reply from stream into presentation element */

	reply = PBSD_rdrpy(c);
	if (reply == NULL) {
		pbs_errno = PBSE_PROTOCOL;
	} else if (reply->brp_choice != BATCH_REPLY_CHOICE_NULL  &&
		reply->brp_choice != BATCH_REPLY_CHOICE_Text &&
		reply->brp_choice != BATCH_REPLY_CHOICE_Status) {
		pbs_errno = PBSE_PROTOCOL;
	} else if (get_conn_errno(c) == 0) {
		rbsp = reply->brp_un.brp_statc;
		reply->brp_un.brp_statc = NULL;
	}
	if (last)
		*last = reply ? reply->last : NULL;
	PBSD_FreeReply(reply);
	return rbsp;
}
