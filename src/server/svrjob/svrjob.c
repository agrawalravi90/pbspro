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

#include <pbs_config.h>

#include <errno.h>

#include "acct.h"
#include "batch_request.h"
#include "job.h"
#include "libpbs.h"
#include "list_link.h"
#include "log.h"
#include "pbs_error.h"
#include "pbs_entlim.h"
#include "pbs_nodes.h"
#include "reservation.h"
#include "server.h"
#include "svrfunc.h"
#include "svrjob.h"
#include "tpp.h"

extern int is_called_by_job_purge;
extern char *msg_job_end_stat;
extern struct server server;
extern pbs_db_conn_t *svr_db_conn;
extern char server_name[];
extern char *msg_abt_err;
extern char *path_spool;

extern int concat_rescused_to_buffer(char **buffer, int *buffer_size, const svrattrl *patlist, char *delim, const svrjob_t *pjob);
extern void free_job_work_tasks(svrjob_t *);
extern void remove_stdouterr_files(svrjob_t *pjob, char *suffix);
extern int remove_mom_ipaddresses_list(mominfo_t *pmom);

static void post_resv_purge(struct work_task *pwt);
static void job_or_resv_init_wattr(void *, int);

/**
 * @brief
 * 		svrjob_alloc - allocate space for a job structure and initialize working
 *				attribute to "unset"
 *
 * @return	pointer to structure or null is space not available.
 */

svrjob_t *
svrjob_alloc(void)
{
	svrjob_t *pj;

	time_t ctm;

	pj = (svrjob_t *) malloc(sizeof(svrjob_t));
	if (pj == NULL) {
		log_err(errno, "svrjob_alloc", "no memory");
		return NULL;
	}
	memset((char *) pj, (int) 0, (size_t) sizeof(svrjob_t));

	CLEAR_LINK(pj->ji_alljobs);
	CLEAR_LINK(pj->ji_jobque);
	CLEAR_LINK(pj->ji_unlicjobs);

	pj->ji_rerun_preq = NULL;
	pj->ji_discarding = 0;
	pj->ji_prunreq = NULL;
	pj->ji_pmt_preq = NULL;
	CLEAR_HEAD(pj->ji_svrtask);
	CLEAR_HEAD(pj->ji_rejectdest);
	pj->ji_terminated = 0;
	pj->ji_deletehistory = 0;
	pj->ji_newjob = 0;
	pj->ji_modified = 0;
	pj->ji_script = NULL;
	pj->ji_qs.ji_jsversion = JSVERSION;
	pj->ji_momhandle = -1;		/* mark mom connection invalid */
	pj->ji_mom_prot = PROT_INVALID; /* invalid protocol type */

	/* set the working attributes to "unspecified" */

	job_init_wattr(pj);

	/* mark as JOB_INITIAL, set accrue times to zero */
	pj->ji_wattr[(int) JOB_ATR_accrue_type].at_val.at_long = JOB_INITIAL;
	pj->ji_wattr[(int) JOB_ATR_eligible_time].at_val.at_long = 0;
	/* start accruing time from the time job was created */
	time(&ctm);
	pj->ji_wattr[(int) JOB_ATR_sample_starttime].at_val.at_long = (long) ctm;

	/* if eligible_time_enable is not true, then job does not accrue eligible time */
	if (server.sv_attr[(int) SRV_ATR_EligibleTimeEnable].at_val.at_long == 1) {
		pj->ji_wattr[(int) JOB_ATR_accrue_type].at_flags |= ATR_VFLAG_SET;

		pj->ji_wattr[(int) JOB_ATR_eligible_time].at_flags |= ATR_VFLAG_SET;

		pj->ji_wattr[(int) JOB_ATR_sample_starttime].at_flags |= ATR_VFLAG_SET;
	}

	return (pj);
}

/**
 * @brief
 * 		svrjob_free - free job structure and its various sub-structures
 *
 * @param[in]	pj - pointer to job structure
 *
 * @return	void
 */
void
svrjob_free(svrjob_t *pj)
{
	int i;
	badplace *bp;

	/* remove any malloc working attribute space */
	for (i = 0; i < (int) JOB_ATR_LAST; i++) {
		job_attr_def[i].at_free(&pj->ji_wattr[i]);
	}

	free_job_work_tasks(pj);

	/* free any bad destination structs */
	bp = (badplace *) GET_NEXT(pj->ji_rejectdest);
	while (bp) {
		delete_link(&bp->bp_link);
		free(bp);
		bp = (badplace *) GET_NEXT(pj->ji_rejectdest);
	}

	if (pj->ji_qs.ji_svrflags & JOB_SVFLG_SubJob) {
		if ((pj->ji_parentaj) && (pj->ji_parentaj->ji_ajtrk))
			pj->ji_parentaj->ji_ajtrk->tkm_tbl[pj->ji_subjindx].trk_psubjob = NULL;
	} else if (pj->ji_ajtrk) {
		/* if Arrayjob, free the tracking table structure */
		for (i = 0; i < pj->ji_ajtrk->tkm_ct; i++) {
			svrjob_t *psubj = pj->ji_ajtrk->tkm_tbl[i].trk_psubjob;
			if (psubj)
				psubj->ji_parentaj = NULL;
		}
		free(pj->ji_ajtrk);
		pj->ji_ajtrk = NULL;
	}
	pj->ji_parentaj = NULL;
	if (pj->ji_discard)
		free(pj->ji_discard);
	if (pj->ji_acctrec)
		free(pj->ji_acctrec);
	if (pj->ji_clterrmsg)
		free(pj->ji_clterrmsg);
	if (pj->ji_script)
		free(pj->ji_script);

	/* if a subjob (of a Array Job), do not free certain items */
	/* which are malloced and shared with the parent Array Job */
	/* They will be freed when the parent is removed           */

	pj->ji_qs.ji_jobid[0] = 'X'; /* as a "freed" marker */
	free(pj);		     /* now free the main structure */
}

/**
 * @brief
 * 		job_purge_svr - purge job from system
 *
 * 		The job is dequeued; the job control file, script file and any spooled
 * 		output files are unlinked, and the job structure is freed.
 * 		If we are MOM, the task files and checkpoint files are also
 * 		removed.
 *
 * @param[in]	pj - pointer to job structure
 *
 * @return	void
 */

void
job_purge_svr(svrjob_t *pjob)
{
	extern char *msg_err_purgejob_db;
	pbs_db_obj_info_t obj;
	pbs_db_job_info_t dbjob;
	pbs_db_conn_t *conn = (pbs_db_conn_t *) svr_db_conn;

	if (pjob->ji_rerun_preq != NULL) {
		log_joberr(PBSE_INTERNAL, __func__, "rerun request outstanding",
			pjob->ji_qs.ji_jobid);
		reply_text(pjob->ji_rerun_preq, PBSE_INTERNAL, "job rerun");
		pjob->ji_rerun_preq = NULL;
	}

	if (pjob->ji_pmt_preq != NULL) {
		log_joberr(PBSE_INTERNAL, __func__, "preempt request outstanding",
			   pjob->ji_qs.ji_jobid);
		reply_preempt_jobs_request(PBSE_INTERNAL, PREEMPT_METHOD_DELETE, pjob);
	}

	if ((pjob->ji_qs.ji_substate != JOB_SUBSTATE_TRANSIN) &&
		(pjob->ji_qs.ji_substate != JOB_SUBSTATE_TRANSICM)) {
		if ((pjob->ji_qs.ji_svrflags & JOB_SVFLG_SubJob) && (pjob->ji_qs.ji_state != JOB_STATE_FINISHED)) {
			if ((pjob->ji_qs.ji_substate == JOB_SUBSTATE_RERUN3) || (pjob->ji_qs.ji_substate == JOB_SUBSTATE_QUEUED))
				update_subjob_state(pjob, JOB_STATE_QUEUED);
			else {
				if (pjob->ji_terminated && pjob->ji_parentaj && pjob->ji_parentaj->ji_ajtrk)
					pjob->ji_parentaj->ji_ajtrk->tkm_dsubjsct++;
				update_subjob_state(pjob, JOB_STATE_EXPIRED);
			}
		}

		(void)account_entity_limit_usages(pjob, NULL, NULL, DECR,
				pjob->ji_etlimit_decr_queued ? ETLIM_ACC_ALL_MAX : ETLIM_ACC_ALL);

		svr_dequejob(pjob);
	}

	/* server code */
	remove_stdouterr_files(pjob, JOB_STDOUT_SUFFIX);
	remove_stdouterr_files(pjob, JOB_STDERR_SUFFIX);

	/* delete job and dependants from database */
	obj.pbs_db_obj_type = PBS_DB_JOB;
	obj.pbs_db_un.pbs_db_job = &dbjob;
	strcpy(dbjob.ji_jobid, pjob->ji_qs.ji_jobid);
	if (pbs_db_delete_obj(conn, &obj) == -1) {
		log_joberr(-1, __func__, msg_err_purgejob_db,
			pjob->ji_qs.ji_jobid);
	}

	if (pjob->ji_qs.ji_svrflags & JOB_SVFLG_HasNodes) {
		is_called_by_job_purge = 1;
		free_nodes(pjob);
		is_called_by_job_purge = 0;
	}

	del_job_related_file(pjob, JOB_CRED_SUFFIX);

	/* Clearing purge job info from svr_newjobs list */
	if (pjob == (svrjob_t *) GET_NEXT(svr_newjobs))
		delete_link(&pjob->ji_alljobs);

	svrjob_free(pjob);

	return;
}

/**
 * @brief
 * 		find_svrjob() - find job by jobid
 *
 *		Search list of all server jobs for one with same job id
 *		Return NULL if not found or pointer to job struct if found.
 *
 *		If the host portion of the job ID contains a dot, it is
 *		assumed that the string represents the FQDN. If no dot is
 *		present, the string represents the short (unqualified)
 *		hostname. For example, "foo" will match "foo.bar.com", but
 *		"foo.bar" will not match "foo.bar.com".
 *
 *		If server, then search in AVL tree otherwise Linked list.
 *
 * @param[in]	jobid - job ID string.
 *
 * @return	pointer to job struct
 * @retval NULL	- if job by jobid not found.
 */

svrjob_t *
find_svrjob(char *jobid)
{
	size_t len;
	AVL_IX_REC *pkey;
	char *host_dot;
	char *serv_dot;
	char *host;
	char *at;
	svrjob_t *pj = NULL;
	char buf[PBS_MAXSVRJOBID + 1];

	if (jobid == NULL)
		return NULL;

	/* Make a copy of the job ID string before we modify it. */
	snprintf(buf, sizeof(buf), "%s", jobid);
	/*
	 * If @server_name was specified, it was used to route the
	 * request to this server. It will not be part of the string
	 * we are searching for, so truncate the string at the '@'
	 * character.
	 */
	if ((at = strchr(buf, (int) '@')) != NULL)
		*at = '\0';

	/*
	 * Avl tree search cannot find partially formed jobid's.
	 * While storing we supplied the full jobid.
	 * So while retrieving also we have to provide
	 * the exact key that was used while storing the job
	 */
	if ((host_dot = strchr(buf, '.')) != NULL) {
		/* The job ID string contains a host string */
		host = host_dot + 1;
		if (strncasecmp(server_name, host, PBS_MAXSERVERNAME + 1) != 0) {
			/*
			 * The server_name and host strings do not match.
			 * Try to determine if one is the FQDN and the other
			 * is the short name. If there is no match, do not
			 * modify the string we will be searching for. If
			 * there is a match, replace host with server_name.
			 *
			 * Do not call is_same_host() to avoid DNS lookup
			 * because server_name may not resolve to a real
			 * host when PBS_SERVER_HOST_NAME is set or when
			 * failover is enabled. The lookup could hang the
			 * server for some amount of time.
			 */
			host_dot = strchr(host, '.');
			serv_dot = strchr(server_name, '.');
			if (host_dot != NULL) {
				/* the host string is FQDN */
				if (serv_dot == NULL) {
					/* the server_name is not FQDN */
					len = strlen(server_name);
					if (len == (host_dot - host)) {
						if (strncasecmp(host, server_name, len) == 0) {
							/* Use server_name to ensure cases match. */
							strcpy(host, server_name);
						}
					}
				}
			} else if (serv_dot != NULL) {
				/* the host string is not FQDN */
				/* the server_name is FQDN */
				len = strlen(host);
				if (len == (serv_dot - server_name)) {
					if (strncasecmp(host, server_name, len) == 0) {
						/* Use server_name to ensure cases match. */
						strcpy(host, server_name);
					}
				}
			}
		} else {
			/*
			 * Case insensitive compare was successful.
			 * Use server_name to ensure cases match.
			 */
			strcpy(host, server_name);
		}
	} else {
		/* The job ID string does not contain a host string */
		strcat(buf, ".");
		strcat(buf, server_name);
	}

	if ((AVL_jctx != NULL) && ((pkey = svr_avlkey_create(buf)) != NULL)) {
		if (avl_find_key(pkey, AVL_jctx) == AVL_IX_OK)
			pj = (svrjob_t *) pkey->recptr;
		free(pkey);
		return (pj);
	}
	pj = (svrjob_t *) GET_NEXT(svr_alljobs);
	while (pj != NULL) {
		if (!strncasecmp(jobid, pj->ji_qs.ji_jobid, sizeof(pj->ji_qs.ji_jobid)))
			break;
		pj = (svrjob_t *) GET_NEXT(pj->ji_alljobs);
	}
	return (pj); /* may be a null pointer */
}

/**
 * @brief   Setter function for JOB_ATR_resc_used_acct
 *
 * @param[in]   pjob - pointer to the job object
 *
 * @return void
 */
void
set_attr_rsc_used_acct(svrjob_t *pjob)
{
    if ((pjob->ji_wattr[JOB_ATR_resc_used_acct].at_flags & ATR_VFLAG_SET) != 0) {
        job_attr_def[JOB_ATR_resc_used_acct].at_free(&pjob->ji_wattr[JOB_ATR_resc_used_acct]);
        pjob->ji_wattr[JOB_ATR_resc_used_acct].at_flags &= ~ATR_VFLAG_SET;
    }

    job_attr_def[JOB_ATR_resc_used_acct].at_set(&pjob->ji_wattr[JOB_ATR_resc_used_acct], &pjob->ji_wattr[JOB_ATR_resc_used], INCR);
}

/**
 * @brief
 * Create the resources_used information for accounting
 *
 * @param[in]	pjob	- pointer to job structure
 * @param[in]	resc_used - pointer to resources used string
 * @param[in]	resc_used_size - size of resources used string
 *
 * @return	int
 * @retval	0 upon success
 * @retval	-1	if error encountered.
 *
 */
int
create_acct_resc_used(const svrjob_t *pjob, char **resc_used, int *resc_used_size)
{
    struct svrattrl *patlist = NULL;
    pbs_list_head temp_head;
    CLEAR_HEAD(temp_head);

    if (pjob->ji_wattr[(int)JOB_ATR_resc_used].at_user_encoded != NULL)
        patlist = pjob->ji_wattr[(int)JOB_ATR_resc_used].at_user_encoded;
    else if (pjob->ji_wattr[(int)JOB_ATR_resc_used].at_priv_encoded != NULL)
        patlist = pjob->ji_wattr[(int)JOB_ATR_resc_used].at_priv_encoded;
    else
        encode_resc(&pjob->ji_wattr[(int)JOB_ATR_resc_used],
                    &temp_head, job_attr_def[(int)JOB_ATR_resc_used].at_name,
                    NULL, ATR_ENCODE_CLIENT, &patlist);
    /*
	 * NOTE:
	 * Following code for constructing resources used information is same as job_obit()
	 * with minor different that to traverse patlist in this code
	 * we have to use patlist->al_sister since it is encoded information in job struct
	 * where in job_obit() we are using GET_NEXT(patlist->al_link) which is part of batch
	 * request.
	 * ji_acctrec is lost on server restart.  Recreate it here if needed.
	 */
    while (patlist)
    {
        /* log to accounting_logs only if there's a value */
        if (strlen(patlist->al_value) > 0)
        {
            if (concat_rescused_to_buffer(resc_used, resc_used_size, patlist, " ", NULL) != 0)
            {
                return -1;
            }
        }
        patlist = patlist->al_sister;
    }
    free_attrlist(&temp_head);
    return 0;
}

/**
 * @brief   Setter function for ji_acctrec
 *
 * @param[in]   pjob - pointer to the job
 *
 * @return void
 */
void
set_acct_resc_used(svrjob_t *pjob)
{
    char *resc_used = NULL;
    int resc_used_size = 0;

    /* Allocate initial space for resc_used.  Future space will be allocated by pbs_strcat(). */
    resc_used = malloc(RESC_USED_BUF_SIZE);
    if (resc_used == NULL)
        return;
    resc_used_size = RESC_USED_BUF_SIZE;

    /* strlen(msg_job_end_stat) == 12 characters plus a number.  This should be plenty big */
    snprintf(resc_used, resc_used_size, msg_job_end_stat, pjob->ji_qs.ji_un.ji_exect.ji_exitstat);

    if (create_acct_resc_used(pjob, &resc_used, &resc_used_size) == -1) {
        free(resc_used);
        return;
    }

    if (pjob->ji_acctrec != NULL)
        free(pjob->ji_acctrec);

    pjob->ji_acctrec = resc_used;
}

/**
 * @brief
 * 		job_abt - abort a job
 *
 *		The job removed from the system and a mail message is sent
 *		to the job owner.
 *
 * @param[in]	pjob - pointer to job structure
 * @param[in]	text - job status message
 */

int
job_abt(svrjob_t *pjob, char *text)
{
	int old_state;
	int old_substate;
	int rc = 0;

	if (pjob == NULL)
		return 0; /* nothing to do */
	/* save old state and update state to Exiting */

	old_state = pjob->ji_qs.ji_state;

	if (old_state == JOB_STATE_FINISHED)
		return 0; /* nothing to do for this job */

	old_substate = pjob->ji_qs.ji_substate;

	/* notify user of abort if notification was requested */

	if (text) { /* req_delete sends own mail and acct record */
		account_record(PBS_ACCT_ABT, pjob, text);
		svr_mailowner(pjob, MAIL_ABORT, MAIL_NORMAL, text);
	}

	if ((old_state == JOB_STATE_RUNNING) && (old_substate != JOB_SUBSTATE_PROVISION)) {
		(void) svr_setjobstate(pjob,
				       JOB_STATE_RUNNING, JOB_SUBSTATE_ABORT);
		rc = issue_signal(pjob, "SIGKILL", release_req, 0);
		if (rc != 0) {
			(void) sprintf(log_buffer, msg_abt_err,
				       pjob->ji_qs.ji_jobid, old_substate);
			log_err(-1, __func__, log_buffer);
			if ((pjob->ji_qs.ji_svrflags & JOB_SVFLG_HERE) == 0) {
				/* notify creator that job is exited */
				pjob->ji_wattr[(int) JOB_ATR_state].at_val.at_char = 'E';
				issue_track(pjob);
			}
			/*
			 * Check if the history of the finished job can be saved or it needs to be purged .
			 */
			svr_saveorpurge_finjobhist(pjob);
		}
	} else if ((old_state == JOB_STATE_TRANSIT) &&
		   (old_substate == JOB_SUBSTATE_TRNOUT)) {
		/* I don't know of a case where this could happen */
		(void) sprintf(log_buffer, msg_abt_err,
			       pjob->ji_qs.ji_jobid, old_substate);
		log_err(-1, __func__, log_buffer);
	} else if (old_substate == JOB_SUBSTATE_PROVISION) {
		(void) svr_setjobstate(pjob, JOB_STATE_RUNNING,
				       JOB_SUBSTATE_ABORT);
		/*
		 * Check if the history of the finished job can be saved or it needs to be purged .
		 */
		svr_saveorpurge_finjobhist(pjob);
	} else if (old_state == JOB_STATE_HELD && old_substate == JOB_SUBSTATE_DEPNHOLD &&
		   (pjob->ji_wattr[(int) JOB_ATR_depend].at_flags & ATR_VFLAG_SET)) {
		(void) svr_setjobstate(pjob, JOB_STATE_HELD,
				       JOB_SUBSTATE_ABORT);
		depend_on_term(pjob);
		/*
		 * Check if the history of the finished job can be saved or it needs to be purged .
		 */
		svr_saveorpurge_finjobhist(pjob);
	} else {
		(void) svr_setjobstate(pjob, JOB_STATE_EXITING,
				       JOB_SUBSTATE_ABORT);
		if ((pjob->ji_qs.ji_svrflags & JOB_SVFLG_HERE) == 0) {
			/* notify creator that job is exited */
			issue_track(pjob);
		}
		/*
		 * Check if the history of the finished job can be saved or it needs to be purged .
		 */
		svr_saveorpurge_finjobhist(pjob);
	}

	return (rc);
}

/**
 * @brief
 * 	free work tasks and pending batch requests related to this job
 *
 * @param[in]	pj - pointer to job structure
 *
 * @return	void
 */
void
free_job_work_tasks(svrjob_t *pj)
{
	struct work_task *pwt;
	struct batch_request *tbr = NULL;
	/*
	* Delete any work task entries associated with the job.
	* mom deferred tasks via TPP are also hooked into the
	* ji_svrtask now, so they also get automatically cleared
	* in this following loop
	*/
	while ((pwt = (struct work_task *) GET_NEXT(pj->ji_svrtask)) != NULL) {
		if (pwt->wt_type == WORK_Deferred_Reply) {
			tbr = (struct batch_request *) pwt->wt_parm1;
			if (tbr != NULL) {
				/* Check if the reply is for scheduler
				 * If so, then reject the request.
				 */
				if ((tbr->rq_orgconn != -1) &&
				    (find_sched_from_sock(tbr->rq_orgconn) != NULL)) {
					tbr->rq_conn = tbr->rq_orgconn;
					req_reject(PBSE_HISTJOBID, 0, tbr);
				}
				/*
				* free batch request from task struct
				* if task is deferred reply
				*/
				else
					free_br(tbr);
			}
		}

		/* wt_event2 either has additional data (like msgid) or NULL */
		free(pwt->wt_event2);

		delete_task(pwt);
	}
}

char *
get_job_credid(char *jobid)
{
#if defined(PBS_SECURITY) && (PBS_SECURITY == KRB5)
	svrjob_t *pjob;

	if ((pjob = find_svrjob(jobid)) == NULL)
		return NULL;

	if (pjob->ji_wattr[(int) JOB_ATR_cred_id].at_flags & ATR_VFLAG_SET) {
		return pjob->ji_wattr[(int) JOB_ATR_cred_id].at_val.at_str;
	}
#endif

	return NULL;
}

/**
 * @brief
 *      spool_filename - formulate stdout/err file name in the spool area.
 *
 * @param[in]    pjob     - pointer to job structure.
 * @param[out]   namebuf  - output/error file name.
 * @param[in]    suffix   - output/error file name suffix.
 *
 * @return  void
 */
void
spool_filename(svrjob_t *pjob, char *namebuf, char *suffix)
{
	if (*pjob->ji_qs.ji_fileprefix != '\0')
		(void) strcat(namebuf, pjob->ji_qs.ji_fileprefix);
	else
		(void) strcat(namebuf, pjob->ji_qs.ji_jobid);
	(void) strcat(namebuf, suffix);
}

/**
 * @brief
 * 		remove_stdouter_files - remove stdout/err files from the spool directory
 *
 * @param[in]   pjob    - pointer to job structure
 * @param[in]	suffix	- output/error file name suffix.
 *
 * @return	void
 */
void
remove_stdouterr_files(svrjob_t *pjob, char *suffix)
{
	char namebuf[MAXPATHLEN + 1];
	extern char *msg_err_purgejob;

	(void) strcpy(namebuf, path_spool);
	spool_filename(pjob, namebuf, suffix);
	if (unlink(namebuf) < 0)
		if (errno != ENOENT)
			log_joberr(errno, __func__, msg_err_purgejob, pjob->ji_qs.ji_jobid);
}

/**
 * @brief
 * 		job_or_resv_init_wattr - initialize job (resc_resv) working attribute array
 *		set the types and the "unspecified value" flag
 *
 * @see
 * 		resc_resv_alloc
 *
 * @param[in]	pobj - job (resc_resv) working attribute array
 * @param[in]	obj_type - This value decides the type of pobj attribute.
 *
 * @return	void
 */
static void
job_or_resv_init_wattr(void *pobj, int obj_type)
{
	int	i;
	int		attr_final;
	attribute	*wattr;
	attribute_def	*p_attr_def;

	if (obj_type == RESC_RESV_OBJECT) {
		attr_final = RESV_ATR_LAST;
		wattr = ((resc_resv *)pobj)->ri_wattr;
		p_attr_def = resv_attr_def;
	} else {
		attr_final = JOB_ATR_LAST;
		wattr = ((svrjob_t *)pobj)->ji_wattr;
		p_attr_def = job_attr_def;
	}

	for (i=0; i<attr_final; i++) {
		clear_attr(&wattr[i], &p_attr_def[i]);
	}
}


/**
 * @brief
 * 		functons for Reservation (resc_resv) structures
 * 		resc_resv_alloc - allocate space for a "resc_resv" structure and initialize
 *		appropriately
 *
 * @return	resc_resv *
 * @retval	nonzero	- successful
 * @retval	0	- unsuccessful
 */

resc_resv *
resc_resv_alloc(void)
{
	resc_resv *resvp;

	resvp = (resc_resv *) malloc(sizeof(resc_resv));
	if (resvp == NULL) {
		log_err(errno, "resc_resv_alloc", "no memory");
		return NULL;
	}
	(void) memset((char *) resvp, (int) 0, (size_t) sizeof(resc_resv));
	CLEAR_LINK(resvp->ri_allresvs);
	CLEAR_HEAD(resvp->ri_svrtask);
	CLEAR_HEAD(resvp->ri_rejectdest);

	/* set the reservation structure's version number and
	 * the working attributes to "unspecified"
	 */

	resvp->ri_qs.ri_rsversion = RSVERSION;
	job_or_resv_init_wattr((void *) resvp, RESC_RESV_OBJECT);

	return (resvp);
}

/**
 * @brief
 * 		resv_free - deals only with the actual "freeing" of a reservation,
 *		accounting, notifying, removing the reservation from linked lists
 *		are handled before hand by resv_abt, resv_purge.  This just frees
 *		any hanging substructures, deletes any attached work_tasks and frees
 *		the resc_resv	structure itself.
 *
 * @param[in,out]		presv - reservation struct which needs to be freed.
 *
 * @return void
 */

void
resv_free(resc_resv *presv)
{
	int i;
	struct work_task *pwt;
	badplace *bp;

	/* remove any malloc working attribute space */

	for (i = 0; i < (int) RESV_ATR_LAST; i++) {
		resv_attr_def[i].at_free(&presv->ri_wattr[i]);
	}

	/* delete any work task entries associated with the resv */

	while ((pwt = (struct work_task *) GET_NEXT(presv->ri_svrtask)) != 0) {
		delete_task(pwt);
	}

	/* free any bad destination structs */
	/* We may never use this code if reservations can't be routed */

	bp = (badplace *) GET_NEXT(presv->ri_rejectdest);
	while (bp) {
		delete_link(&bp->bp_link);
		free(bp);
		bp = (badplace *) GET_NEXT(presv->ri_rejectdest);
	}

	/* any "interactive" batch request? (shouldn't be); free it now */
	if (presv->ri_brp)
		free_br(presv->ri_brp);

	/* now free the main structure */
	free(presv);
}

/**
 * @brief
 * 		find_resv() - find resc_resv struct by reservation ID
 *
 *		Search list of all server resc_resv structs for one with same
 *		reservation ID as input "resvID"
 *
 * @param[in]	resvID - reservation ID
 *
 * @return	pointer to resc_resv struct
 * @retval	NULL	- not found
 */

resc_resv *
find_resv(char *resvID)
{
	char *at;
	resc_resv *presv;

	if ((at = strchr(resvID, (int) '@')) != 0)
		*at = '\0'; /* strip of @server_name */
	presv = (resc_resv *) GET_NEXT(svr_allresvs);
	while (presv != NULL) {
		if (!strcmp(resvID, presv->ri_qs.ri_resvID))
			return (presv);
		presv = (resc_resv *) GET_NEXT(presv->ri_allresvs);
	}
	if (at)
		*at = '@'; /* restore @server_name */

	return (presv); /* pointer value is null */
}

/**
 * @brief
 *  	post_resv_purge - As with the other "post_*" functions, this
 *		handles the return reply from an internally generated request.
 *		Function resv_purge() ended up having to generate an internal
 *		request to qmgr() to delete the reservation's attached queue.
 *		When the reply to that is received and indicates success,
 *		resv_purge() will be re-called and this time the latter half
 *		of the resv_purge() code will execute to finish the purge.
 *		Otherwise, the reservation just won't get purged.  It will
 *		just be defunct
 *
 * @param[in]	pwt - work structure which contains internally generated request.
 *
 * @return void
 */

static void
post_resv_purge(struct work_task *pwt)
{
	int code;
	resc_resv *presv;
	struct batch_request *preq;

	preq = (struct batch_request *) pwt->wt_parm1;
	presv = (resc_resv *) pwt->wt_parm2;
	code = preq->rq_reply.brp_code;

	/*Release the batch_request hanging (wt_parm1) from the
	 *work_task structure
	 */
	release_req(pwt);

	if (code) {
		/*response from the request is that an error occured
		 *So, we failed on deleting the reservation's queue -
		 *should mail owner about the failure
		 */
		return;
	}

	/*qmgr gave no error in doing MGR_CMD_DELETE on the queue
	 *So it's safe to clear the reservation's queue pointer
	 */
	presv->ri_qp = NULL;

	/*now re-call resv_purge to execute the function's lower part*/
	resv_purge(presv);
}


/**
 * @brief
 * 		resv_purge - purge reservation from system
 *
 * 		The reservation is unlinked from the server's svr_allresvs;
 * 		the reservation control file is unlinked, any attached work_task's
 * 		are deleted and the resc_resv structure is freed along with any
 * 		hanging, malloc'd memory areas.
 *
 * 		This function - ASSUMES - that if the reservation is supported by a
 * 		pbs_queue that queue is empty OR having history jobs only (i.e. job
 * 		in state JOB_STATE_MOVED/JOB_STATE_FINISHED). So, whatever mechanism
 * 		is being used to remove the jobs from such a supporting queue should,
 * 		at the outset, store the value "False" into the queue's "enabled"
 * 		attribute (blocks new jobs from being placed in the queue while the
 * 		server attempts to delete those currently in the queue) and into its
 * 		"scheduling" attribute (to disable servicing by the scheduler).
 *
 * 		Any hanging, empty pbs_queue will be handled by creating and issuing
 * 		to the server a PBS_BATCH_Manager request to delete this queue.  This
 * 		will be dispatched immediately and a work_task having a function whose
 * 		sole job is to free the batch_request struct is placed on the "immediate"
 * 		task list, for processing by the "next_task" function in the main loop
 * 		of the server.
 *
 * 		This function should only be called after a check has been made to
 * 		to verify that the party deleting the reservation has proper permission
 *
 * @param[in]	presv - pointer to reservation which needs to be puged.
 *
 * @return	void
 */
void
resv_purge(resc_resv *presv)
{
	struct batch_request *preq;
	struct work_task *pwt;
	extern char *msg_purgeResvFail;
	extern char *msg_purgeResvDb;
	pbs_db_obj_info_t obj;
	pbs_db_resv_info_t dbresv;

	if (presv == NULL)
		return;

	if (presv->ri_qp != NULL) {
		/*
		 * Issue a batch_request to remove the supporting pbs_queue
		 * As Stated: Assumption is that the queue is empty of jobs
		 */
		preq = alloc_br(PBS_BATCH_Manager);
		if (preq == NULL) {
			(void) sprintf(log_buffer, "batch request allocation failed");
			log_event(PBSEVENT_SYSTEM, PBS_EVENTCLASS_RESV, LOG_ERR,
				  presv->ri_qs.ri_resvID, log_buffer);
			return;
		}

		CLEAR_LINK(preq->rq_ind.rq_manager.rq_attr);
		preq->rq_ind.rq_manager.rq_cmd = MGR_CMD_DELETE;
		preq->rq_ind.rq_manager.rq_objtype = MGR_OBJ_QUEUE;

		(void) strcpy(preq->rq_user, "pbs_server");
		(void) strcpy(preq->rq_host, pbs_server_name);
		/*
		 * Copy the queue name from the attributes rather than use the
		 * presv->ri_qp->qu_qs.qu_name value. The post_resv_purge()
		 * function could modify it at any time. See SPID 352225.
		 */
		strcpy(preq->rq_ind.rq_manager.rq_objname,
		       presv->ri_wattr[RESV_ATR_queue].at_val.at_str);

		/* It is assumed that the prior check on permission was OK */
		preq->rq_perm |= ATR_DFLAG_MGWR;

		if (issue_Drequest(PBS_LOCAL_CONNECTION, preq, post_resv_purge, &pwt, 0) == -1) {
			/* Failed to delete queue. */
			free_br(preq);
			log_event(PBSEVENT_RESV, PBS_EVENTCLASS_RESV, LOG_WARNING,
				  presv->ri_qs.ri_resvID, msg_purgeResvFail);
			return;
		}
		/*
		 * Queue was deleted. Invocation of post_resv_purge() will
		 * re-call resv_purge() (passing wt_parm2)
		 */
		if (pwt)
			pwt->wt_parm2 = presv;
		return;
	}

	/* reservation no longer has jobs or a supporting queue */

	if (presv->ri_giveback) {
		/*ok, resources were actually assigned to this reservation
		 *and must now be accounted back into the loaner's pool
		 */

		set_resc_assigned((void *) presv, 1, DECR);
		presv->ri_giveback = 0;
	}

	/* Remove reservation's link element from whichever of the server's
	 * global lists (svr_allresvs or svr_newresvs) has it
	 */
	delete_link(&presv->ri_allresvs);

	/* Delete any lingering tasks pointing to this reservation */
	delete_task_by_parm1_func(presv, NULL, DELETE_ALL);

	/* Release any nodes that were associated to this reservation */
	free_resvNodes(presv);
	set_scheduler_flag(SCH_SCHEDULE_TERM, dflt_scheduler);

	strcpy(dbresv.ri_resvid, presv->ri_qs.ri_resvID);
	obj.pbs_db_obj_type = PBS_DB_RESV;
	obj.pbs_db_un.pbs_db_resv = &dbresv;
	if (pbs_db_delete_obj(svr_db_conn, &obj) == -1)
		log_err(errno, __func__, msg_purgeResvDb);

	/* Free resc_resv struct, any hanging substructs, any attached *work_task structs */
	resv_free(presv);
	return;
}

/**
 * @brief
 * 		Set node state to resv-exclusive if either reservation requests
 * 		exclusive placement or the node sharing attribute is to be exclusive or
 * 		reservation requests AOE.
 *
 * @param[in]	presv The reservation being considered
 *
 * @return void
 *
 * @MT-safe: No
 */
void
resv_exclusive_handler(resc_resv *presv)
{
	attribute *patresc;
	resource_def *prsdef;
	resource *pplace;
	pbsnode_list_t *pnl;
	int share_node = VNS_DFLT_SHARED;
	int share_resv = VNS_DFLT_SHARED;
	char *scdsel;

	patresc = &presv->ri_wattr[(int) RESV_ATR_resource];
	prsdef = find_resc_def(svr_resc_def, "place", svr_resc_size);
	pplace = find_resc_entry(patresc, prsdef);
	if (pplace && pplace->rs_value.at_val.at_str) {
		if ((place_sharing_type(pplace->rs_value.at_val.at_str,
					VNS_FORCE_EXCLHOST) != VNS_UNSET) ||
		    (place_sharing_type(pplace->rs_value.at_val.at_str,
					VNS_FORCE_EXCL) != VNS_UNSET)) {
			share_resv = VNS_FORCE_EXCL;
		}
		if (place_sharing_type(pplace->rs_value.at_val.at_str,
				       VNS_IGNORE_EXCL) == VNS_IGNORE_EXCL) {
			share_resv = VNS_IGNORE_EXCL;
		}
	}

	if (share_resv != VNS_FORCE_EXCL) {
		scdsel = presv->ri_wattr[(int) RESV_ATR_SchedSelect].at_val.at_str;
		if (scdsel && strstr(scdsel, "aoe="))
			share_resv = VNS_FORCE_EXCL;
	}
	for (pnl = presv->ri_pbsnode_list; pnl != NULL; pnl = pnl->next) {
		share_node = pnl->vnode->nd_attr[(int) ND_ATR_Sharing].at_val.at_long;

		/*
		 * set node state to resv-exclusive if either node forces exclusive
		 * or reservation requests exclusive and node does not ignore
		 * exclusive.
		 */
		if ((share_node == VNS_FORCE_EXCL) || (share_node == VNS_FORCE_EXCLHOST) ||
		    ((share_node != VNS_IGNORE_EXCL) && (share_resv == VNS_FORCE_EXCL)) ||
		    (((share_node == VNS_DFLT_EXCL) || (share_node == VNS_DFLT_EXCLHOST)) && (share_resv != VNS_IGNORE_EXCL))) {
			set_vnode_state(pnl->vnode, INUSE_RESVEXCL, Nd_State_Or);
		}
	}
}

/**
 * @brief
 *  	Find aoe from the reservation request
 *
 * @see
 *		resc_select_action
 *
 * @param[in]	presv	- pointer to the reservation
 *
 * @return	char *
 * @retval	NULL     - no aoe requested
 * @retval	NON NULL - value of aoe requested
 *
 * @par Side Effects:
 *	Memory returned is to be freed by caller
 *
 * @par MT-safe: yes
 *
 */
char *
find_aoe_from_request(resc_resv *presv)
{
	char *aoe_req = NULL;
	char *p, *q;
	int i = 0;

	/* look into schedselect as this is expanded form of select
	 * after taking into account default_chunk.res.
	 */
	if (presv == NULL)
		return NULL;

	if (presv->ri_wattr[(int) RESV_ATR_SchedSelect].at_val.at_str) {
		/* just get first appearance of aoe */
		q = presv->ri_wattr[(int) RESV_ATR_SchedSelect].at_val.at_str;
		if ((p = strstr(q, "aoe=")) != NULL) {
			p += 4; /* strlen("aoe=") = 4 */
			/* get length of aoe name in i. */
			for (q = p; *q && *q != ':' && *q != '+'; i++, q++)
				;
			aoe_req = malloc(i + 1);
			if (aoe_req == NULL) {
				log_err(ENOMEM, __func__, "out of memory");
				return NULL;
			}
			strncpy(aoe_req, p, i);
			aoe_req[i] = '\0';
		}
	}
	return aoe_req;
}

/**
 * @brief
 * 		delete_svrmom_entry - destroy a mom_svrinfo_t element and the parent
 *		mominfo_t element.  This special function is required because of
 *		the msr_addrs array hung off of the mom_svrinfo_t
 *
 * @see
 * 		effective_node_delete
 *
 * @param[in]	pmom	- pointer to mominfo structure
 *
 * @return	void
 */

void
delete_svrmom_entry(mominfo_t *pmom)
{
	mom_svrinfo_t *psvrmom = NULL;
	unsigned long *up;
	extern struct tree *ipaddrs;

	if (pmom->mi_data) {
		/* send request to this mom to delete all hooks known from this server. */
		/* we'll just send this delete request only once */
		/* if a hook fails to delete, then that mom host when it */
		/* come back will still have the hook. */
		if ((pmom->mi_action != NULL) && (mom_hooks_seen_count() > 0)) {
			/* there should be at least one hook to */
			/* add mom actions below, which are in behalf of */
			/* existing hooks. */
			(void) bg_delete_mom_hooks(pmom);
		}

		psvrmom = (mom_svrinfo_t *) pmom->mi_data;
		if (psvrmom->msr_arch)
			free(psvrmom->msr_arch);

		if (psvrmom->msr_pbs_ver)
			free(psvrmom->msr_pbs_ver);

		if (psvrmom->msr_addrs) {
			for (up = psvrmom->msr_addrs; *up; up++) {
				/* del Mom's IP addresses from tree  */
				tdelete2(*up, pmom->mi_port, &ipaddrs);
			}
			free(psvrmom->msr_addrs);
			psvrmom->msr_addrs = NULL;
		}
		if (psvrmom->msr_children)
			free(psvrmom->msr_children);

		if (psvrmom->msr_jobindx) {
			free(psvrmom->msr_jobindx);
			psvrmom->msr_jbinxsz = 0;
			psvrmom->msr_jobindx = NULL;
		}

		/* take stream out of tree */
		tpp_close(psvrmom->msr_stream);
		tdelete2((unsigned long) psvrmom->msr_stream, 0, &streams);

		if (remove_mom_ipaddresses_list(pmom) != 0) {
			snprintf(log_buffer, sizeof(log_buffer), "Could not remove IP address for mom %s:%d from cache",
				 pmom->mi_host, pmom->mi_port);
			log_err(errno, __func__, log_buffer);
		}
	}
	memset((void *) psvrmom, 0, sizeof(mom_svrinfo_t));
	psvrmom->msr_stream = -1; /* always set to -1 when deleted */
	delete_mom_entry(pmom);
}
