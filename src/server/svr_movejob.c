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

#include <pbs_config.h>   /* the master config generated by configure */

#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <signal.h>
#include <sys/types.h>

#include <unistd.h>
#include <fcntl.h>
#include <time.h>
#include <sys/time.h>
#include <sys/param.h>
#include <sys/wait.h>
#include <netdb.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include "libpbs.h"
#include "pbs_error.h"
#include "list_link.h"
#include "attribute.h"
#include "server_limits.h"
#include "work_task.h"
#include "log.h"
#include "pbs_db.h"
#include "batch_request.h"
#include "resv_node.h"
#include "queue.h"
#include "job.h"
#include "reservation.h"
#include "credential.h"
#include "ticket.h"
#include "queue.h"
#include "job.h"
#include "net_connect.h"
#include "pbs_nodes.h"
#include "svrfunc.h"
#include "tpp.h"
#include <memory.h>
#include "server.h"
#include "hook.h"
#include "pbs_sched.h"
#include "acct.h"


#define	RETRY	3	/* number of times to retry network move */

/* External functions called */

extern void	stat_mom_job(job *);
int  local_move(job *, struct batch_request *);

/* Private Functions local to this file */

static void post_movejob(struct work_task *);
static void post_routejob(struct work_task *);
static int small_job_files(job* pjob);
extern int should_retry_route(int err);
extern int move_job_file(int con, job *pjob, enum job_file which, int prot, char **msgid);
extern void post_sendmom(struct work_task *pwt);


/* Global Data */

#if !defined(H_ERRNO_DECLARED)
extern int     h_errno;
#endif
extern char	*path_jobs;
extern char	*path_spool;
extern attribute_def job_attr_def[];
extern char	*msg_badexit;
extern char	*msg_routebad;
extern char	*msg_routexceed;
extern char	*msg_manager;
extern char	*msg_movejob;
extern char     *msg_err_malloc;
extern int	comp_resc_gt, comp_resc_eq, comp_resc_lt;
extern int	pbs_errno;
extern pbs_net_t pbs_server_addr;
extern unsigned int pbs_server_port_dis;
extern int	resc_access_perm;
extern time_t	time_now;
extern int svr_create_tmp_jobscript(job *pj, char *script_name);
extern int	scheduler_jobs_stat;
extern	char	*path_hooks_workdir;
extern struct work_task *add_mom_deferred_list(int stream, mominfo_t *minfo, void (*func)(), char *msgid, void *parm1, void *parm2);


/**
 * @brief
 * 		svr_movejob - Test if the destination is local or not and call a routine to
 * 		do the appropriate move.
 *
 * param[in,out]	jobp	-	pointer to job to move
 * param[in]	destination	-	destination to be moved
 * param[in]	req	-	client request from a qmove client, null if a route
 *
 * @return	int
 * @retval	0	: success
 * @retval	-1	: permenent failure or rejection,
 * @retval	1	: failed but try again
 * @reval	2	: deferred (ie move in progress), check later
 */
int
svr_movejob(job	*jobp, char *destination, struct batch_request *req)
{
	unsigned int port = pbs_server_port_dis;
	char	*toserver;


	if (strlen(destination) >= (size_t)PBS_MAXROUTEDEST) {
		sprintf(log_buffer, "name %s over maximum length of %d",
			destination, PBS_MAXROUTEDEST);
		log_err(-1, "svr_movejob", log_buffer);
		pbs_errno = PBSE_QUENBIG;
		return -1;
	}

	strncpy(jobp->ji_qs.ji_destin, destination, PBS_MAXROUTEDEST);
	jobp->ji_qs.ji_un_type = JOB_UNION_TYPE_ROUTE;

	if ((toserver = strchr(destination, '@')) != NULL) {
		/* check to see if the part after '@' is this server */
		int comp = -1;
		comp = comp_svraddr(pbs_server_addr, parse_servername(++toserver, &port));
		if ((comp == 1) ||
			(port != pbs_server_port_dis)) {
			return (net_move(jobp, req));	/* not a local dest */
		}
		else if (comp == 2)
			return -1;
	}

	/* if get to here, it is a local destination */
	return (local_move(jobp, req));
}


/**
 * @brief
 * 		Move a job to another queue in this Server.
 *
 * @par
 * 		Check the destination to see if it can accept the job.
 * 		If the job can enter the new queue, dequeue from the existing queue and
 * 		enqueue into the new queue
 *
 * @par
 * 		Note - the destination is specified by the queue's name in the
 *		ji_qs.ji_destin element of the job structure.
 *
 * param[in]	jobp	-	pointer to job to move
 * param[in]	req	-	client request from a qmove client, null if a route
 *
 * @return	int
 * @retval  0	: success
 * @retval -1	: permanent failure or rejection, see pbs_errno
 * @retval  1	: failed but try again later
 */
int
local_move(job *jobp, struct batch_request *req)
{
	pbs_queue *qp;
	char	  *destination = jobp->ji_qs.ji_destin;
	int	   mtype;
	attribute *pattr;
	long	newtype = -1;
	long	time_msec;
	struct timeval	tval;

	/* search for destination queue */
	if ((qp = find_queuebyname(destination)) == NULL) {
		sprintf(log_buffer,
			"queue %s does not exist",
			destination);
		log_err(-1, __func__, log_buffer);
		pbs_errno = PBSE_UNKQUE;
		return -1;
	}

	/*
	 * if being moved at specific request of administrator, then
	 * checks on queue availability, etc. are skipped;
	 * otherwise all checks are enforced.
	 */

	if (req == NULL) {
		mtype = MOVE_TYPE_Route;	/* route */
	} else if (req->rq_perm & (ATR_DFLAG_MGRD | ATR_DFLAG_MGWR)) {
		mtype =	MOVE_TYPE_MgrMv;	/* privileged move */
	} else {
		mtype = MOVE_TYPE_Move;		/* non-privileged move */
	}

	pbs_errno = svr_chkque(jobp, qp, get_hostPart(jobp->ji_wattr[(int)JOB_ATR_job_owner].at_val.at_str), mtype);
	if (pbs_errno) {
		/* should this queue be retried? */
		return (should_retry_route(pbs_errno));
	}

	/* dequeue job from present queue, update destination and	*/
	/* queue rank for new queue and enqueue into destination	*/

	svr_dequejob(jobp);
	jobp->ji_myResv = NULL;
	strncpy(jobp->ji_qs.ji_queue, qp->qu_qs.qu_name, PBS_MAXQUEUENAME);
	jobp->ji_qs.ji_queue[PBS_MAXQUEUENAME] = '\0';

	gettimeofday(&tval, NULL);
	time_msec = (tval.tv_sec * 1000L) + (tval.tv_usec/1000L);

	jobp->ji_wattr[(int)JOB_ATR_qrank].at_val.at_long = time_msec;
	jobp->ji_wattr[(int)JOB_ATR_qrank].at_flags |= ATR_MOD_MCACHE;

	pattr = &jobp->ji_wattr[(int)JOB_ATR_reserve_ID];
	if (qp->qu_resvp) {
		job_attr_def[(int)JOB_ATR_reserve_ID].at_decode(pattr, NULL, NULL, qp->qu_resvp->ri_qs.ri_resvID);
		jobp->ji_myResv = qp->qu_resvp;
	} else {
		job_attr_def[(int)JOB_ATR_reserve_ID].at_decode(pattr, NULL, NULL, NULL);
	}

	if (server.sv_attr[(int)SVR_ATR_EligibleTimeEnable].at_val.at_long == 1) {
		newtype = determine_accruetype(jobp);
		update_eligible_time(newtype, jobp);
	}


	if ((pbs_errno = svr_enquejob(jobp)) != 0)
		return -1;		/* should never ever get here */
	account_jobstr(jobp, PBS_ACCT_QUEUE);

	jobp->ji_lastdest = 0;	/* reset in case of another route */

	job_save_db(jobp);
	

	/* If a scheduling cycle is in progress, then this moved job may have
	 * had changes resulting from the move that would impact scheduling or
	 * placement, add job to list of jobs which cannot be run in this cycle.
	 */
	if ((req == NULL || (find_sched_from_sock(req->rq_conn) == NULL)) && (scheduler_jobs_stat))
		am_jobs_add(jobp);

	return 0;
}

/**
 * @brief
 * 		post_routejob - clean up action for child started in net_move/send_job
 *		   to "route" a job to another server
 * @par
 * 		If route was successfull, delete job.
 * @par
 * 		If route didn't work, mark destination not to be tried again for this
 * 		job and call route again.
 *
 * @param[in]	pwt	-	work task structure
 *
 * @return	none.
 */
static void
post_routejob(struct work_task *pwt)
{
	char	 newstate;
	int	 newsub;
	int	 r;
	int	 stat = pwt->wt_aux;
	job	*jobp = (job *)pwt->wt_parm2;

	if (jobp == NULL) {
		log_event(PBSEVENT_ERROR, PBS_EVENTCLASS_JOB, LOG_INFO, "", "post_routejob failed, jobp NULL");
		return;
	}

	if (WIFEXITED(stat)) {
		r = WEXITSTATUS(stat);
	} else {
		r = SEND_JOB_FATAL;
		(void)sprintf(log_buffer, msg_badexit, stat);
		(void)strcat(log_buffer, __func__);
		log_event(PBSEVENT_SYSTEM, PBS_EVENTCLASS_JOB, LOG_NOTICE,
			jobp->ji_qs.ji_jobid, log_buffer);
	}

	switch (r) {
		case SEND_JOB_OK:		/* normal return, job was routed */

			if (jobp->ji_qs.ji_svrflags & JOB_SVFLG_StagedIn)
				remove_stagein(jobp);
			/*
			 * If the server is configured to keep job history and the job
			 * is created here, do not purge the job structure but save
			 * it for history purpose. No need to check for sub-jobs as
			 * sub jobs can not be routed.
			 */
			if (svr_chk_history_conf())
				svr_setjob_histinfo(jobp, T_MOV_JOB);
			else
				job_purge(jobp); /* need to remove server job struct */
			return;
		case SEND_JOB_FATAL:		/* permanent rejection (or signal) */
			if (check_job_substate(jobp, JOB_SUBSTATE_ABORT)) {

				/* Job Delete in progress, just set to queued status */

				(void)svr_setjobstate(jobp, JOB_STATE_LTR_QUEUED,
					JOB_SUBSTATE_ABORT);
				return;
			}
			add_dest(jobp);		/* else mark destination as bad */
			/* fall through */
		default :	/* try routing again */
			/* force re-eval of job state out of Transit */
			svr_evaljobstate(jobp, &newstate, &newsub, 1);
			(void)svr_setjobstate(jobp, newstate, newsub);
			jobp->ji_retryok = 1;
			if ((r = job_route(jobp)) == PBSE_ROUTEREJ)
				(void)job_abt(jobp, msg_routebad);
			else if (r != 0)
				(void)job_abt(jobp, msg_routexceed);
			break;
	}
	return;
}

/**
 * @brief
 * 		post_movejob - clean up action for child started in net_move/send_job
 *		   to "move" a job to another server
 * @par
 * 		If move was successfull, delete server's copy of thejob structure,
 * 		and reply to request.
 * @par
 * 		If route didn't work, reject the request.
 *
 * @param[in]	pwt	-	work task structure
 *
 * @return	none.
 */
static void
post_movejob(struct work_task *pwt)
{
	struct batch_request *req;
	char	newstate;
	int	newsub;
	int	stat;
	int	r;
	job	*jobp;

	req  = (struct batch_request *)pwt->wt_parm1;
	stat = pwt->wt_aux;
	pbs_errno = PBSE_NONE;
	if (req->rq_type != PBS_BATCH_MoveJob) {
		sprintf(log_buffer, "bad request type %d", req->rq_type);
		log_err(-1, __func__, log_buffer);
		return;
	}

	jobp = find_job(req->rq_ind.rq_move.rq_jid);
	if ((jobp == NULL) || (jobp != (job *)pwt->wt_parm2)) {
		sprintf(log_buffer,
			"job %s not found",
			req->rq_ind.rq_move.rq_jid);
		log_err(-1, __func__, log_buffer);

	}

	if (WIFEXITED(stat)) {
		r = WEXITSTATUS(stat);
		if (r == SEND_JOB_OK) {	/* purge server's job structure */
			if (jobp->ji_qs.ji_svrflags & JOB_SVFLG_StagedIn)
				remove_stagein(jobp);
			(void)strcpy(log_buffer, msg_movejob);
			(void)sprintf(log_buffer+strlen(log_buffer),
				msg_manager,
				req->rq_ind.rq_move.rq_destin,
				req->rq_user, req->rq_host);
			/*
			 * If server is configured to keep job history info and
			 * the job is created here, then keep the job struture
			 * for history purpose without purging. No need to check
			 * for sub-jobs as sub jobs can't be moved.
			 */
			if (svr_chk_history_conf())
				svr_setjob_histinfo(jobp, T_MOV_JOB);
			else
				job_purge(jobp);
		} else
			r = PBSE_ROUTEREJ;
	} else {
		r = PBSE_SYSTEM;
		(void)sprintf(log_buffer, msg_badexit, stat);
		(void)strcat(log_buffer, __func__);
		log_event(PBSEVENT_SYSTEM, PBS_EVENTCLASS_JOB, LOG_NOTICE,
			jobp->ji_qs.ji_jobid, log_buffer);
	}

	if (r) {
		if (jobp) {
			/* force re-eval of job state out of Transit */
			svr_evaljobstate(jobp, &newstate, &newsub, 1);
			svr_setjobstate(jobp, newstate, newsub);
		}
		req_reject(r, 0, req);
	} else
		reply_ack(req);

	return;
}

/**
 *
 * @brief
 * 	Send execution job on connected tpp stream.
 *	Note: Job structure has been loaded with the script by now (ji_script populated)
 *
 * @param[in]	jobp - pointer to the job being sent
 * @param[in]	hostaddr - the address of host to send job to, host byte order
 * @param[in]	port - the destination port, host byte order
 * @param[in]	request - The batch request associated with this send job call
 *
 * @return int
 * @retval  2 	success
 * @retval  -1 	failure (pbs_errno set to error number)
 *
 */
int
send_job_exec(job *jobp, pbs_net_t hostaddr, int port, struct batch_request *request)
{
	pbs_list_head attrl;
	attribute *pattr;
	mominfo_t *pmom = NULL;
	int stream = -1;
	int encode_type;
	char *destin = jobp->ji_qs.ji_destin;
	int i;
	size_t credlen = 0;
	char *credbuf = NULL;
	char job_id[PBS_MAXSVRJOBID + 1];
	struct attropl *pqjatr; /* list (single) of attropl for quejob */
	int rc;
	char *jobid = NULL;
	char *msgid = NULL;
	char *dup_msgid = NULL;
	struct work_task *ptask = NULL;
	int save_resc_access_perm;
	char *extend = NULL;

	/* saving resc_access_perm global variable as backup */
	save_resc_access_perm = resc_access_perm;
	pbs_errno = PBSE_NONE;

	stream = svr_connect(hostaddr, port, NULL, ToServerDIS, PROT_TPP);
	if (stream < 0) {
		sprintf(log_buffer, "Could not connect to Mom, svr_connect returned %d", stream);
		log_event(PBSEVENT_ERROR, PBS_EVENTCLASS_REQUEST, LOG_WARNING, "", log_buffer);
		goto send_err;
	}

	pmom = tfind2((unsigned long) jobp->ji_qs.ji_un.ji_exect.ji_momaddr,
		jobp->ji_qs.ji_un.ji_exect.ji_momport,
		&ipaddrs);
	if (!pmom || (((mom_svrinfo_t *)(pmom->mi_data))->msr_state & INUSE_DOWN)) {
		log_event(PBSEVENT_ERROR, PBS_EVENTCLASS_REQUEST, LOG_WARNING, "", "Mom is down");
		pbs_errno = PBSE_NORELYMOM;
		goto send_err;
	}

	CLEAR_HEAD(attrl);

	resc_access_perm = ATR_DFLAG_MOM;
	encode_type = ATR_ENCODE_MOM;

	pattr = jobp->ji_wattr;
	for (i = 0; i < (int) JOB_ATR_LAST; i++) {
		if ((job_attr_def + i)->at_flags & resc_access_perm) {
			(void)(job_attr_def + i)->at_encode(pattr + i, &attrl,
				(job_attr_def + i)->at_name, NULL, encode_type,
				NULL);
		}
	}
	attrl_fixlink(&attrl);
	/* save the job id for when after we purge the job */

	/* read any credential file */
	(void)get_credential(pmom->mi_host, jobp, PBS_GC_BATREQ, &credbuf, &credlen);

	(void) strcpy(job_id, jobp->ji_qs.ji_jobid);

	if (((jobp->ji_qs.ji_svrflags & JOB_SVFLG_SCRIPT) == 0) && (credlen <= 0) && ((jobp->ji_qs.ji_svrflags & JOB_SVFLG_HASRUN) == 0))
		extend = EXTEND_OPT_IMPLICIT_COMMIT;

	pqjatr = &((svrattrl *) GET_NEXT(attrl))->al_atopl;
	jobid = PBSD_queuejob(stream, jobp->ji_qs.ji_jobid, destin, pqjatr, extend, PROT_TPP, &msgid, NULL);
	free_attrlist(&attrl);
	if (jobid == NULL)
		goto send_err;

	tpp_add_close_func(stream, process_DreplyTPP); /* register a close handler */

	/* adding msgid to deferred list, dont free msgid */
	if ((ptask = add_mom_deferred_list(stream, pmom, post_sendmom, msgid, request, jobp)) == NULL) {
		log_event(PBSEVENT_ERROR, PBS_EVENTCLASS_REQUEST, LOG_WARNING, "", "add_mom_deferred_list returned NULL");
		pbs_errno = PBSE_SYSTEM;
		goto send_err;
	}

	/* add to pjob->svrtask list so its automatically cleared when job is purged */
	append_link(&jobp->ji_svrtask, &ptask->wt_linkobj, ptask);

	/* 
	 * svr-mom communication is asynchronous, so PBSD_quejob does not return in this flow
	 * hence commit_done is meaningless here
	 * We only need to check extend and skip sending the other messages
	 */
	if (extend)
		goto done;

	/* we cannot use the same msgid, since it is not part of the preq,
	 * make a dup of it, and we can freely free it
	 */
	if ((dup_msgid = strdup(msgid)) == NULL) {
		log_event(PBSEVENT_ERROR, PBS_EVENTCLASS_REQUEST, LOG_WARNING, "", "strdup returned NULL");
		pbs_errno = PBSE_SYSTEM;
		goto send_err;
	}

	/*
	 * henceforth use the same msgid, since we mean to say all this is
	 * part of a single logical request to the mom
	 * and we will be hanging off one request to be answered to finally
	 */
	if (jobp->ji_qs.ji_svrflags & JOB_SVFLG_SCRIPT) {
		if (PBSD_jscript_direct(stream, jobp->ji_script, PROT_TPP, &dup_msgid) != 0)
			goto send_err;
	}
	if (jobp->ji_script) {
		free(jobp->ji_script);
		jobp->ji_script = NULL;
	}

	if (credlen > 0) {
		rc = PBSD_jcred(stream, jobp->ji_extended.ji_ext.ji_credtype, credbuf, credlen, PROT_TPP, &dup_msgid);
		if (credbuf)
			free(credbuf);
		if (rc != 0)
			goto send_err;
	}

	if ((jobp->ji_qs.ji_svrflags & JOB_SVFLG_HASRUN) && (hostaddr != pbs_server_addr)) {
		if ((move_job_file(stream, jobp, StdOut, PROT_TPP, &dup_msgid) != 0) ||
			(move_job_file(stream, jobp, StdErr, PROT_TPP, &dup_msgid) != 0) ||
			(move_job_file(stream, jobp, Chkpt, PROT_TPP, &dup_msgid) != 0))
			goto send_err;
	}

	if (PBSD_commit(stream, job_id, PROT_TPP, &dup_msgid) != 0)
		goto send_err;

done:
	free(dup_msgid); /* free this as it is not part of any work task */
	resc_access_perm = save_resc_access_perm; /* reset back to it's old value */
	return 2;

send_err:
	if (dup_msgid)
		free(dup_msgid);

	if (jobp->ji_script) {
		free(jobp->ji_script);
		jobp->ji_script = NULL;
	}

	if (ptask) {
		if (ptask->wt_event2)
			free(ptask->wt_event2);
		delete_task(ptask);
	}

	sprintf(log_buffer, "send of job to %s failed error = %d", destin, pbs_errno);
	log_event(PBSEVENT_JOB, PBS_EVENTCLASS_JOB, LOG_INFO, jobp->ji_qs.ji_jobid, log_buffer);
	resc_access_perm = save_resc_access_perm; /* reset back to it's old value */
	return (-1);
}

/**
 *
 * @brief
 * 		Send a job over the network to some other server or MOM.
 * @par
 * 		Under Linux/Unix, this starts a child process to do the work.
 *		Connect to the destination host and port,
 * 		and go through the protocol to transfer the job.
 * 		Signals are blocked.
 *
 * @param[in]	jobp	-	pointer to the job being sent.
 * @param[in]	hostaddr	-	the address of host to send job to, host byte order.
 * @param[in]	port	-	the destination port, host byte order
 * @param[in]	move_type	-	the type of move (e.g. MOVE_TYPE_exec)
 * @param[in]	post_func	-	the function to execute once the child process
 *								sending job completes (Linux/Unix only)
 * @param[in]	data	-	input data to 'post_func'
 *
 * @return	int
 * @retval	2	parent	: success (child forked)
 * @retval	-1	parent	: on failure (pbs_errno set to error number)
 * @retval	SEND_JOB_OK	child	: 0 success, job sent
 * @retval	SEND_JOB_FATAL	child	: 1 permenent failure or rejection,
 * @retval	SEND_JOB_RETRY	child	: 2 failed but try again
 * @retval	SEND_JOB_NODEDW child	: 3 execution node down, retry different node
 */
int
send_job(job *jobp, pbs_net_t hostaddr, int port, int move_type,
	void (*post_func)(struct work_task *), struct batch_request *preq)
{
	pbs_list_head attrl;
	enum conn_type cntype = ToServerDIS;
	int con;
	char *credbuf = NULL;
	size_t credlen = 0;
	char *destin = jobp->ji_qs.ji_destin;
	int encode_type;
	int i;
	char job_id[PBS_MAXSVRJOBID + 1];
	attribute *pattr;
	pid_t pid;
	struct attropl *pqjatr; /* list (single) of attropl for quejob */
	char script_name[MAXPATHLEN + 1];
	struct work_task *ptask;
	struct hostent *hp;
	struct in_addr addr;
	long tempval;

	/* if job has a script read it from database */
	if (jobp->ji_qs.ji_svrflags & JOB_SVFLG_SCRIPT) {
		if (svr_load_jobscript(jobp) == NULL) {
			pbs_errno = PBSE_SYSTEM;
			snprintf(log_buffer, sizeof(log_buffer),
					"Failed to load job script for job %s",
					jobp->ji_qs.ji_jobid);
			log_err(pbs_errno, __func__, log_buffer);
			return (-1);
		}
	}

	if (move_type == MOVE_TYPE_Exec && small_job_files(jobp)) {
		return (send_job_exec(jobp, hostaddr, port, preq));
	}

	(void)snprintf(log_buffer, sizeof(log_buffer), "big job files, sending via subprocess");
	log_event(PBSEVENT_DEBUG3, PBS_EVENTCLASS_JOB, LOG_INFO, jobp->ji_qs.ji_jobid, log_buffer);

	script_name[0] = '\0';
	/* if job has a script read it from database */
	if (jobp->ji_qs.ji_svrflags & JOB_SVFLG_SCRIPT) {
		/* write the job script to a temporary file */
		if (svr_create_tmp_jobscript(jobp, script_name) != 0) {
			pbs_errno = PBSE_SYSTEM;
			snprintf(log_buffer, sizeof(log_buffer),
				"Failed to create temporary job script for job %s",
				jobp->ji_qs.ji_jobid);
			log_err(pbs_errno, __func__, log_buffer);

			if (jobp->ji_script) {
				free(jobp->ji_script);
				jobp->ji_script = NULL;
			}

			return -1;
		}
	}

	if (jobp->ji_script) {
		free(jobp->ji_script);
		jobp->ji_script = NULL;
	}

	pid = fork();
	if (pid == -1) {	/* Error on fork */
		log_err(errno, __func__, "fork failed\n");
		pbs_errno = PBSE_SYSTEM;
		return -1;
	}

	if (pid != 0) {		/* The parent (main server) */

		ptask = set_task(WORK_Deferred_Child, pid, post_func, preq);
		if (!ptask) {
			log_err(errno, __func__, msg_err_malloc);
			return (-1);
		} else {
			ptask->wt_parm2 = jobp;
			append_link(&((job *)jobp)->ji_svrtask,
				&ptask->wt_linkobj, ptask);
		}
		return 2;
	}

	/*
	 * the child process
	 *
	 * set up signal cather for error return
	 */
	DBPRT(("%s: child started, sending to port %d\n", __func__, port))
	tpp_terminate();

	/* Unprotect child from being killed by kernel */
	daemon_protect(0, PBS_DAEMON_PROTECT_OFF);
		addr.s_addr = htonl(hostaddr);
		hp = gethostbyaddr((void *)&addr, sizeof(struct in_addr), AF_INET);
		if (hp == NULL) {
			sprintf(log_buffer, "%s: h_errno=%d",
				inet_ntoa(addr), h_errno);
			log_err(-1, __func__, log_buffer);
		} else {
			/* read any credential file */
			(void)get_credential(hp->h_name, jobp, PBS_GC_BATREQ,
				&credbuf, &credlen);
		}
	/* encode job attributes to be moved */

	CLEAR_HEAD(attrl);

	/* select attributes/resources to send based on move type */

	if (move_type == MOVE_TYPE_Exec) {
		resc_access_perm = ATR_DFLAG_MOM;
		encode_type = ATR_ENCODE_MOM;
		cntype = ToServerDIS;
	} else {
		resc_access_perm = ATR_DFLAG_USWR | ATR_DFLAG_OPWR |
			ATR_DFLAG_MGWR | ATR_DFLAG_SvRD;
		encode_type = ATR_ENCODE_SVR;
		svr_dequejob(jobp);	/* clears default resource settings */
	}

	/* our job is to calc eligible time accurately and save it */
	/* on new server, accrue type should be calc afresh */
	/* Note: if job is being sent for execution on mom, then don't calc eligible time */

	if ((jobp->ji_wattr[(int)JOB_ATR_accrue_type].at_val.at_long == JOB_ELIGIBLE) &&
		(server.sv_attr[(int)SVR_ATR_EligibleTimeEnable].at_val.at_long == 1) &&
		(move_type != MOVE_TYPE_Exec)) {
		tempval = ((long)time_now - jobp->ji_wattr[(int)JOB_ATR_sample_starttime].at_val.at_long);
		jobp->ji_wattr[(int)JOB_ATR_eligible_time].at_val.at_long += tempval;
		jobp->ji_wattr[(int)JOB_ATR_eligible_time].at_flags |= ATR_MOD_MCACHE;
	}

	pattr = jobp->ji_wattr;
	for (i=0; i < (int)JOB_ATR_LAST; i++) {
		if ((job_attr_def+i)->at_flags & resc_access_perm) {
			(void)(job_attr_def+i)->at_encode(pattr+i, &attrl,
				(job_attr_def+i)->at_name, NULL,
				encode_type, NULL);
		}
	}
	attrl_fixlink(&attrl);


	/* save the job id for when after we purge the job */

	(void)strcpy(job_id, jobp->ji_qs.ji_jobid);

	pbs_errno = 0;
	con = -1;

	for (i=0; i<RETRY; i++) {

		/* connect to receiving server with retries */

		if (i > 0) {	/* recycle after an error */
			if (con >= 0)
				svr_disconnect(con);
			if (should_retry_route(pbs_errno) == -1) {
				/* delete the temp script file */
				unlink(script_name);
				exit(SEND_JOB_FATAL);	/* fatal error, don't retry */
			}
			sleep(1<<i);
		}
		if ((con = svr_connect(hostaddr, port, 0, cntype, PROT_TCP)) == PBS_NET_RC_FATAL) {
			log_errf(pbs_errno, __func__, "send_job failed to %lx port %d",
				hostaddr, port);

			/* delete the temp script file */
			unlink(script_name);

			if ((move_type == MOVE_TYPE_Exec) && (pbs_errno == PBSE_BADCRED))
				exit(SEND_JOB_NODEDW);

			exit(SEND_JOB_FATAL);
		} else if (con == PBS_NET_RC_RETRY) {
			pbs_errno = ECONNREFUSED;	/* should retry */
			continue;
		}

		/*
		 * if the job is substate JOB_SUBSTATE_TRNOUTCM which means
		 * we are recovering after being down or a late failure, we
		 * just want to send the commit"
		 */

		if (!check_job_substate(jobp, JOB_SUBSTATE_TRNOUTCM)) {

			if (!check_job_substate(jobp, JOB_SUBSTATE_TRNOUT))
				set_job_substate(jobp, JOB_SUBSTATE_TRNOUT);

			pqjatr = &((svrattrl *)GET_NEXT(attrl))->al_atopl;
			if (PBSD_queuejob(con, jobp->ji_qs.ji_jobid, destin, pqjatr, NULL, PROT_TCP, NULL, NULL) == 0) {
				if (pbs_errno == PBSE_JOBEXIST &&
					move_type == MOVE_TYPE_Exec) {
					/* already running, mark it so */
					log_event(PBSEVENT_ERROR, PBS_EVENTCLASS_JOB, LOG_INFO, jobp->ji_qs.ji_jobid, "Mom reports job already running");
					exit(SEND_JOB_OK);
				}
				else if ((pbs_errno == PBSE_HOOKERROR) || (pbs_errno == PBSE_HOOK_REJECT)  ||
					(pbs_errno == PBSE_HOOK_REJECT_RERUNJOB) || (pbs_errno == PBSE_HOOK_REJECT_DELETEJOB)) {
					char name_buf[MAXPATHLEN + 1];
					int rfd;
					int len;
					char *reject_msg;
					int err;

					err = pbs_errno;

					reject_msg = pbs_geterrmsg(con);
					(void)sprintf(log_buffer, "send of job to %s failed error = %d reject_msg=%s", destin, err, reject_msg ? reject_msg : "");
					log_event(PBSEVENT_JOB, PBS_EVENTCLASS_JOB, LOG_INFO, jobp->ji_qs.ji_jobid, log_buffer);

					(void)strcpy(name_buf, path_hooks_workdir);
					(void)strcat(name_buf, jobp->ji_qs.ji_jobid);
					(void)strcat(name_buf, HOOK_REJECT_SUFFIX);

					if ((reject_msg != NULL) && (reject_msg[0] != '\0')) {
						if ((rfd = open(name_buf, O_RDWR|O_CREAT|O_TRUNC, 0600)) == -1) {
							sprintf(log_buffer, "open of reject file %s failed: errno %d", name_buf, errno);
							log_event(PBSEVENT_JOB, PBS_EVENTCLASS_JOB, LOG_INFO, jobp->ji_qs.ji_jobid, log_buffer);
						} else {
							len = strlen(reject_msg)+1;
							/* write also trailing null char */
							if (write(rfd, reject_msg, len) != len) {
								sprintf(log_buffer, "write to file %s incomplete: errno %d", name_buf, errno);
								log_event(PBSEVENT_JOB, PBS_EVENTCLASS_JOB, LOG_INFO, jobp->ji_qs.ji_jobid, log_buffer);
							}
							close(rfd);
						}
					}

					if (err == PBSE_HOOKERROR)
						exit(SEND_JOB_HOOKERR);
					if (err == PBSE_HOOK_REJECT)
						exit(SEND_JOB_HOOK_REJECT);
					if (err == PBSE_HOOK_REJECT_RERUNJOB)
						exit(SEND_JOB_HOOK_REJECT_RERUNJOB);
					if (err == PBSE_HOOK_REJECT_DELETEJOB)
						exit(SEND_JOB_HOOK_REJECT_DELETEJOB);
				}
				else {
					(void)sprintf(log_buffer, "send of job to %s failed error = %d", destin, pbs_errno);
					log_event(PBSEVENT_JOB, PBS_EVENTCLASS_JOB, LOG_INFO, jobp->ji_qs.ji_jobid, log_buffer);
					continue;
				}
			}

			if (jobp->ji_qs.ji_svrflags & JOB_SVFLG_SCRIPT) {
				if (PBSD_jscript(con, script_name, PROT_TCP, NULL) != 0)
					continue;
			}

			if ((move_type == MOVE_TYPE_Exec) &&
				(jobp->ji_qs.ji_svrflags & JOB_SVFLG_HASRUN) &&
				(hostaddr !=  pbs_server_addr)) {
				/* send files created on prior run */
				if ((move_job_file(con, jobp, StdOut, PROT_TCP, NULL) != 0) ||
					(move_job_file(con, jobp, StdErr, PROT_TCP, NULL) != 0) ||
					(move_job_file(con, jobp, Chkpt, PROT_TCP, NULL) != 0))
					continue;
			}

			set_job_substate(jobp, JOB_SUBSTATE_TRNOUTCM);
		}

		if (PBSD_rdytocmt(con, job_id, PROT_TCP, NULL) != 0)
			continue;

		if (PBSD_commit(con, job_id, PROT_TCP, NULL) != 0) {
			/* delete the temp script file */
			unlink(script_name);
			exit(SEND_JOB_FATAL);
		}

		svr_disconnect(con);

		/* delete the temp script file */
		unlink(script_name);

		exit(SEND_JOB_OK);	/* This child process is all done */
	}
	if (con >= 0)
		svr_disconnect(con);
	/*
	 * If connection is actively refused by the execution node(or mother superior) OR
	 * the execution node(or mother superior) is rejecting request with error
	 * PBSE_BADHOST(failing to authorize server host), the node should be marked down.
	 */
	if ((move_type == MOVE_TYPE_Exec) && (pbs_errno == ECONNREFUSED  || pbs_errno == PBSE_BADHOST)) {
		i = SEND_JOB_NODEDW;
	} else if (should_retry_route(pbs_errno) == -1) {
		i = SEND_JOB_FATAL;
	} else {
		i = SEND_JOB_RETRY;
	}
	(void)sprintf(log_buffer, "send_job failed with error %d", pbs_errno);
	log_event(PBSEVENT_DEBUG, PBS_EVENTCLASS_JOB, LOG_NOTICE, jobp->ji_qs.ji_jobid, log_buffer);

	/* delete the temp script file */
	unlink(script_name);

	exit(i);
	return -1;		/* NOT REACHED */
}

/**
 * @brief
 * 		net_move - move a job over the network to another queue.
 * @par
 * 		Get the address of the destination server and call send_job()
 *
 * @return	int
 * @retval	2	: success (child started, see send_job())
 * @retval	-1	: error
 */

int
net_move(job *jobp, struct batch_request *req)
{
	void		*data;
	char		*destination = jobp->ji_qs.ji_destin;
	pbs_net_t	 hostaddr;
	char		*hostname;
	int		 move_type;
	unsigned int	 port = pbs_server_port_dis;
	void	       (*post_func)(struct work_task *);
	char		*toserver;

	/* Determine to whom are we sending the job */

	if ((toserver = strchr(destination, '@')) == NULL) {
		sprintf(log_buffer,
			"no server specified in %s", destination);
		log_err(-1, __func__, log_buffer);
		return (-1);
	}

	toserver++;		/* point to server name */
	hostname = parse_servername(toserver, &port);
	hostaddr = get_hostaddr(hostname);
	if (req) {
		/* note, in this case, req is the orginal Move Request */
		move_type = MOVE_TYPE_Move;
		post_func = post_movejob;
		data      = req;
	} else {
		/* note, in this case req is NULL */
		move_type = MOVE_TYPE_Route;
		post_func = post_routejob;
		data      = 0;
	}

	(void)svr_setjobstate(jobp, JOB_STATE_LTR_TRANSIT, JOB_SUBSTATE_TRNOUT);
	return (send_job(jobp, hostaddr, port, move_type, post_func, data));
}


/**
 * @brief
 * 		should_retry_route - should the route be retried based on the error return
 * @par
 *		Certain error are temporary, and that destination should not be
 *		considered bad.
 *
 * @param[in]	err	-	error return
 *
 * @return	int
 * @retval	1	: it should retry this destination
 * @retval	-1	: if destination should not be retried
 */

int
should_retry_route(int err)
{
	switch (err) {
		case 0:
		case EADDRINUSE:
		case EADDRNOTAVAIL:
		case ECONNREFUSED:
		case PBSE_JOBEXIST:
		case PBSE_SYSTEM:
		case PBSE_INTERNAL:
		case PBSE_EXPIRED:
		case PBSE_MAXQUED:
		case PBSE_QUNOENB:
		case PBSE_NOCONNECTS:
		case PBSE_ENTLIMCT:
		case PBSE_ENTLIMRESC:
			return (1);

		default:
			return (-1);
	}
}
/**
 * @brief
 * 		move_job_file - send files created on prior run
 *
 * @param[in]	conn	-	connection handle
 * @param[in]	pjob	-	pointer to job structure
 * @param[in]	which	-	standard file type, see libpbs.h
 * @param[in]	prot	-	PROT_TPP or PROT_TCP
 * @param[out]	msgid	-	message id
 *
 * @return	int
 * @retval	0	: success
 * @retval	!=0	: error code
 */
int
move_job_file(int conn, job *pjob, enum job_file which, int prot, char **msgid)
{
	char path[MAXPATHLEN+1];

	(void)strcpy(path, path_spool);
	if (*pjob->ji_qs.ji_fileprefix != '\0')
		(void)strcat(path, pjob->ji_qs.ji_fileprefix);
	else
		(void)strcat(path, pjob->ji_qs.ji_jobid);
	if (which == StdOut)
		(void)strcat(path, JOB_STDOUT_SUFFIX);
	else if (which == StdErr)
		(void)strcat(path, JOB_STDERR_SUFFIX);
	else if (which == Chkpt)
		(void)strcat(path, JOB_CKPT_SUFFIX);

	if (access(path, F_OK) < 0) {
		if (errno == ENOENT)
			return (0);
		else
			return (errno);
	}
	return PBSD_jobfile(conn, PBS_BATCH_MvJobFile, path, pjob->ji_qs.ji_jobid, which, prot, msgid);
}

/**
 * @brief
 * 		cnvrt_local_move - internally move a job to another queue
 * @par
 * 		Check the destination to see if it can accept the job.
 *
 * @return	int
 * @retval	0	: success
 * @retval	-1	: permanent failure or rejection, see pbs_errno
 * @retval	1	: failed but try again
 */
int
cnvrt_local_move(job *jobp, struct batch_request *req)
{
	return (local_move(jobp, req));
}


/**
 * @brief
 * 		check size of job files
 * @par
 * 		Checks the size of the job-script/output/error/checkpoint files for a job.
 * 		If the job is not being rerun, simply returns 1.
 *
 * @return	int
 * @retval	0	: at least one file is larger than 2MB.
 * @retval	1	: all job files are smaller than 2MB.
 */
static int
small_job_files(job* pjob)
{
	int  		max_bytes_over_tpp = 2*1024*1024;
	char 		path[MAXPATHLEN+1] = {0};
	struct stat 	sb;
	int		have_file_prefix = 0;

	if (pjob->ji_script && (strlen(pjob->ji_script) > max_bytes_over_tpp))
		return 0;

	/*
	 * If the job is not being rerun, we need not check
	 * the size of the spool files.
	 */
	if (!(pjob->ji_qs.ji_svrflags & JOB_SVFLG_HASRUN))
		return 1;

	if (*pjob->ji_qs.ji_fileprefix != '\0')
		have_file_prefix = 1;

	if (have_file_prefix)
		snprintf(path, MAXPATHLEN, "%s%s%s", path_spool, pjob->ji_qs.ji_fileprefix, JOB_STDOUT_SUFFIX);
	else
		snprintf(path, MAXPATHLEN, "%s%s%s", path_spool, pjob->ji_qs.ji_jobid, JOB_STDOUT_SUFFIX);
	if ((access(path, F_OK) == 0) && !stat(path, &sb))
		if (sb.st_size > max_bytes_over_tpp)
			return 0;

	memset(path, 0, sizeof(path));
	if (have_file_prefix)
		snprintf(path, MAXPATHLEN, "%s%s%s", path_spool, pjob->ji_qs.ji_fileprefix, JOB_STDERR_SUFFIX);
	else
		snprintf(path, MAXPATHLEN, "%s%s%s", path_spool, pjob->ji_qs.ji_jobid, JOB_STDERR_SUFFIX);
	if ((access(path, F_OK) == 0) && !stat(path, &sb))
		if (sb.st_size > max_bytes_over_tpp)
			return 0;

	memset(path, 0, sizeof(path));
	if (have_file_prefix)
		snprintf(path, MAXPATHLEN, "%s%s%s", path_spool, pjob->ji_qs.ji_fileprefix, JOB_CKPT_SUFFIX);
	else
		snprintf(path, MAXPATHLEN, "%s%s%s", path_spool, pjob->ji_qs.ji_jobid, JOB_CKPT_SUFFIX);
	if ((access(path, F_OK) == 0) && !stat(path, &sb))
		if (sb.st_size > max_bytes_over_tpp)
			return 0;

	return 1;
}
