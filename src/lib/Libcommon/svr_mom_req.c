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

#include <arpa/inet.h>
#include <ctype.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>

#include "acct.h"
#include "attribute.h"
#include "batch_request.h"
#include "credential.h"
#include "dis.h"
#include "job.h"
#include "libpbs.h"
#include "list_link.h"
#include "log.h"
#ifdef PBS_MOM
#include "mom_func.h"
#include "mom_mach.h"
#endif
#include "net_connect.h"
#include "pbs_db.h"
#include "pbs_error.h"
#include "server_limits.h"
#include "svrfunc.h"
#include "tpp.h"
#include "work_task.h"

extern char *msg_err_malloc;
extern pbs_list_head svr_requests;
extern char *msg_jobnew;
extern char *msg_issuebad;
extern char *msg_nosupport;
/**
 * @brief
 *		Commit ownership of job
 * @par Functionality:
 *		Set state of job to JOB_STATE_QUEUED (or Held or Waiting) and
 *		enqueue the job into its destination queue.
 *
 *  @param[in]	preq	-	The batch request structure
 *
 */
void req_commit(struct batch_request *preq)
{
#ifndef PBS_MOM
	int newstate;
	int newsub;
	pbs_queue *pque;
	int rc;
	pbs_db_jobscr_info_t jobscr;
	pbs_db_obj_info_t obj;
	long time_msec;
	svrjob_t *pj;
#ifdef WIN32
	struct _timeb tval;
#else
	struct timeval tval;
#endif
	pbs_db_conn_t *conn = (pbs_db_conn_t *) svr_db_conn;
#else
	job *pj;
#endif

	pj = locate_new_job(preq, preq->rq_ind.rq_commit);
	if (pj == NULL) {
		req_reject(PBSE_UNKJOBID, 0, preq);
		return;
	}

	if (pj->ji_qs.ji_substate != JOB_SUBSTATE_TRANSIN) {
		req_reject(PBSE_IVALREQ, 0, preq);
		return;
	}

	pj->ji_qs.ji_state = JOB_STATE_TRANSIT;
	pj->ji_wattr[(int) JOB_ATR_state].at_val.at_char = 'T';
	pj->ji_wattr[(int) JOB_ATR_state].at_flags |=
	    ATR_VFLAG_SET | ATR_VFLAG_MODCACHE;
	pj->ji_qs.ji_substate = JOB_SUBSTATE_TRANSICM;
	pj->ji_wattr[(int) JOB_ATR_substate].at_flags |=
	    ATR_VFLAG_SET | ATR_VFLAG_MODCACHE;

#ifdef PBS_MOM /* MOM only */

#if IRIX6_CPUSET == 1
	/*
	 * Grab a mutex to allow machine dependent code to block while the
	 * job state is being set up.
	 */
	ACQUIRE_LOCK(*pbs_commit_ptr);
#endif /* IRIX6_CPUSET */
	/* move job from new job list to "all" job list, set to running state */

	delete_link(&pj->ji_alljobs);
	append_link(&svr_alljobs, &pj->ji_alljobs, pj);
	/*
	 ** Set JOB_SVFLG_HERE to indicate that this is Mother Superior.
	 */
	pj->ji_qs.ji_svrflags |= JOB_SVFLG_HERE;

	pj->ji_qs.ji_state = JOB_STATE_RUNNING;
	pj->ji_wattr[(int) JOB_ATR_state].at_flags |= ATR_VFLAG_MODIFY;
	pj->ji_qs.ji_substate = JOB_SUBSTATE_PRERUN;
	pj->ji_wattr[(int) JOB_ATR_substate].at_flags |= ATR_VFLAG_MODIFY;
	pj->ji_qs.ji_un_type = JOB_UNION_TYPE_MOM;
	if (preq->prot) {
		struct sockaddr_in *addr = tpp_getaddr(preq->rq_conn);
		if (addr)
			pj->ji_qs.ji_un.ji_momt.ji_svraddr = (pbs_net_t) ntohl(addr->sin_addr.s_addr);
	} else
		pj->ji_qs.ji_un.ji_momt.ji_svraddr = get_connectaddr(preq->rq_conn);
	pj->ji_qs.ji_un.ji_momt.ji_exitstat = 0;
	if ((pj->ji_qs.ji_svrflags & (JOB_SVFLG_CHKPT | JOB_SVFLG_ChkptMig)) == 0) {
		pj->ji_qs.ji_stime = time_now; /* start of walltime */
		pj->ji_wattr[(int) JOB_ATR_stime].at_flags |= ATR_VFLAG_MODIFY;
	}

	/*
	 * For MOM - reply to the request and start up the job
	 * any errors will be dealt with via the mechanism
	 * used for a terminated job
	 */

	reply_jobid(preq, pj->ji_qs.ji_jobid, BATCH_REPLY_CHOICE_Commit);
	start_exec(pj);
	job_or_resv_save((void *) pj, SAVEJOB_NEW, JOB_OBJECT);

	/* The ATR_VFLAG_MODIFY bit for several attributes used to be
	 * set here. Now we rely on these bits to be set when and where
	 * an attribute is modified. Several of these are also set in
	 * record_finish_exec().
	 */

#if IRIX6_CPUSET == 1
	mom_get_sample(); /* Setup for sampling. */

	RELEASE_LOCK(*pbs_commit_ptr); /* Release the lock. */
#endif				       /* IRIX6_CPUSET */

#else /* PBS_SERVER */
	if (svr_authorize_jobreq(preq, pj) == -1) {
		req_reject(PBSE_PERM, 0, preq);
		return;
	}

	/* Set Server level entity usage */

	if ((rc = account_entity_limit_usages(pj, NULL, NULL, INCR, ETLIM_ACC_ALL)) != 0) {
		job_purge_generic(pj);
		req_reject(rc, 0, preq);
		return;
	}

	/* remove job for the server new job list, set state, and enqueue it */

	delete_link(&pj->ji_alljobs);

	svr_evaljobstate(pj, &newstate, &newsub, 1);
	svr_setjobstate(pj, newstate, newsub);

#ifdef WIN32
	_ftime_s(&tval);
	time_msec = (tval.time * 1000L) + tval.millitm;
#else
	gettimeofday(&tval, NULL);
	time_msec = (tval.tv_sec * 1000L) + (tval.tv_usec / 1000L);
#endif
	/* set the queue rank attribute */
	pj->ji_wattr[(int) JOB_ATR_qrank].at_val.at_long = time_msec;
	pj->ji_wattr[(int) JOB_ATR_qrank].at_flags |=
	    ATR_VFLAG_SET | ATR_VFLAG_MODCACHE;

	if ((rc = svr_enquejob(pj)) != 0) {
		job_purge_generic(pj);
		req_reject(rc, 0, preq);
		return;
	}
	account_jobstr(pj, PBS_ACCT_QUEUE);

	/* save job and job script within single transaction */
	pbs_db_begin_trx(conn, 0, 0);

	/* Make things faster by writing job only once here  - at commit time */
	if (job_or_resv_save((void *) pj, SAVEJOB_NEW, JOB_OBJECT)) {
		(void) pbs_db_end_trx(conn, PBS_DB_ROLLBACK);
		job_purge_generic(pj);
		req_reject(PBSE_SAVE_ERR, 0, preq);
		return;
	}

	strcpy(jobscr.ji_jobid, pj->ji_qs.ji_jobid);
	jobscr.script = pj->ji_script;
	obj.pbs_db_obj_type = PBS_DB_JOBSCR;
	obj.pbs_db_un.pbs_db_jobscr = &jobscr;

	if (pbs_db_save_obj(conn, &obj, PBS_INSERT_DB) != 0) {
		job_purge_generic(pj);
		req_reject(PBSE_SYSTEM, 0, preq);
		(void) pbs_db_end_trx(conn, PBS_DB_ROLLBACK);
		return;
	}
	if (pj->ji_script) {
		free(pj->ji_script);
		pj->ji_script = NULL;
	}

	/* Now, no need to save server here because server
	   has already saved in the get_next_svr_sequence_id() */

	if (pbs_db_end_trx(conn, PBS_DB_COMMIT) != 0) {
		job_purge_generic(pj);
		req_reject(PBSE_SYSTEM, 0, preq);
		return;
	}

	/*
	 * if the job went into a Route (push) queue that has been started,
	 * try once to route it to give immediate feedback as a courtsey
	 * to the user.
	 */

	pque = pj->ji_qhdr;

	if ((preq->rq_fromsvr == 0) &&
	    (pque->qu_qs.qu_type == QTYPE_RoutePush) &&
	    (pque->qu_attr[(int) QA_ATR_Started].at_val.at_long != 0)) {
		if ((rc = job_route(pj)) != 0) {
			job_purge_generic(pj);
			req_reject(rc, 0, preq);
			return;
		}
	}

	/* need to format message first, before request goes away */

	(void) snprintf(log_buffer, sizeof(log_buffer), msg_jobnew,
			preq->rq_user, preq->rq_host,
			pj->ji_wattr[(int) JOB_ATR_job_owner].at_val.at_str,
			pj->ji_wattr[(int) JOB_ATR_jobname].at_val.at_str,
			pj->ji_qhdr->qu_qs.qu_name);

	/* acknowledge the request with the job id */
	if ((rc = reply_jobid(preq, pj->ji_qs.ji_jobid, BATCH_REPLY_CHOICE_Commit))) {
		(void) snprintf(log_buffer, sizeof(log_buffer),
				"Failed to reply with Job Id, error %d", rc);
		log_event(PBSEVENT_JOB, PBS_EVENTCLASS_JOB, LOG_ERR,
			  pj->ji_qs.ji_jobid, log_buffer);
		job_purge_generic(pj);
		return;
	}

	log_event(PBSEVENT_JOB, PBS_EVENTCLASS_JOB, LOG_INFO,
		  pj->ji_qs.ji_jobid, log_buffer);

	if ((pj->ji_qs.ji_svrflags & JOB_SVFLG_HERE) == 0)
		issue_track(pj); /* notify creator where job is */
#endif /* PBS_SERVER */
}

/**
 * @brief
 * 	Read in an DIS encoded request from the network
 * 	and decodes it:
 *	Read and decode the request into the request structures
 *
 * @see
 * 	process_request and read_fo_request
 *
 * @param[in] sfds	- the socket descriptor
 * @param[in,out] request - will contain the decoded request
 *
 * @return int
 * @retval 0 	if request read ok, batch_request pointed to by request is updated.
 * @retval -1 	if EOF (no request but no error)
 * @retval >0 	if errors ( a PBSE_ number)
 */

int
dis_request_read(int sfds, struct batch_request *request)
{
#ifndef PBS_MOM
	int i;
#endif /* PBS_MOM */
	int proto_type;
	int proto_ver;
	int rc; /* return code */

	if (request->prot == PROT_TCP)
		DIS_tcp_funcs(); /* setup for DIS over tcp */

	/* Decode the Request Header, that will tell the request type */

	rc = decode_DIS_ReqHdr(sfds, request, &proto_type, &proto_ver);

	if (rc != 0) {
		if (rc == DIS_EOF)
			return EOF;
		(void) sprintf(log_buffer,
			       "Req Header bad, errno %d, dis error %d",
			       errno, rc);
		log_event(PBSEVENT_DEBUG, PBS_EVENTCLASS_REQUEST, LOG_DEBUG,
			  "?", log_buffer);

		return PBSE_DISPROTO;
	}

	if (proto_ver > PBS_BATCH_PROT_VER)
		return PBSE_DISPROTO;

	/* Decode the Request Body based on the type */

	switch (request->rq_type) {
	case PBS_BATCH_Connect:
		break;

	case PBS_BATCH_Disconnect:
		return (-1); /* set EOF return */

	case PBS_BATCH_QueueJob:
	case PBS_BATCH_SubmitResv:
		CLEAR_HEAD(request->rq_ind.rq_queuejob.rq_attr);
		rc = decode_DIS_QueueJob(sfds, request);
		break;

	case PBS_BATCH_JobCred:
		rc = decode_DIS_JobCred(sfds, request);
		break;

	case PBS_BATCH_UserCred:
		rc = decode_DIS_UserCred(sfds, request);
		break;

	case PBS_BATCH_jobscript:
	case PBS_BATCH_MvJobFile:
		rc = decode_DIS_JobFile(sfds, request);
		break;

	case PBS_BATCH_RdytoCommit:
	case PBS_BATCH_Commit:
	case PBS_BATCH_Rerun:
		rc = decode_DIS_JobId(sfds, request->rq_ind.rq_commit);
		break;

	case PBS_BATCH_DeleteJob:
	case PBS_BATCH_DeleteResv:
	case PBS_BATCH_ResvOccurEnd:
	case PBS_BATCH_HoldJob:
	case PBS_BATCH_ModifyJob:
	case PBS_BATCH_ModifyJob_Async:
		rc = decode_DIS_Manage(sfds, request);
		break;

	case PBS_BATCH_MessJob:
		rc = decode_DIS_MessageJob(sfds, request);
		break;

	case PBS_BATCH_Shutdown:
	case PBS_BATCH_FailOver:
		rc = decode_DIS_ShutDown(sfds, request);
		break;

	case PBS_BATCH_SignalJob:
		rc = decode_DIS_SignalJob(sfds, request);
		break;

	case PBS_BATCH_StatusJob:
		rc = decode_DIS_Status(sfds, request);
		break;

	case PBS_BATCH_PySpawn:
		rc = decode_DIS_PySpawn(sfds, request);
		break;

	case PBS_BATCH_Authenticate:
		rc = decode_DIS_Authenticate(sfds, request);
		break;

#ifndef PBS_MOM
	case PBS_BATCH_RelnodesJob:
		rc = decode_DIS_RelnodesJob(sfds, request);
		break;

	case PBS_BATCH_LocateJob:
		rc = decode_DIS_JobId(sfds, request->rq_ind.rq_locate);
		break;

	case PBS_BATCH_Manager:
	case PBS_BATCH_ReleaseJob:
		rc = decode_DIS_Manage(sfds, request);
		break;

	case PBS_BATCH_MoveJob:
	case PBS_BATCH_OrderJob:
		rc = decode_DIS_MoveJob(sfds, request);
		break;

	case PBS_BATCH_RunJob:
	case PBS_BATCH_AsyrunJob:
	case PBS_BATCH_StageIn:
	case PBS_BATCH_ConfirmResv:
		rc = decode_DIS_Run(sfds, request);
		break;

	case PBS_BATCH_DefSchReply:
		request->rq_ind.rq_defrpy.rq_cmd = disrsi(sfds, &rc);
		if (rc)
			break;
		request->rq_ind.rq_defrpy.rq_id = disrst(sfds, &rc);
		if (rc)
			break;
		request->rq_ind.rq_defrpy.rq_err = disrsi(sfds, &rc);
		if (rc)
			break;
		i = disrsi(sfds, &rc);
		if (rc)
			break;
		if (i)
			request->rq_ind.rq_defrpy.rq_txt = disrst(sfds, &rc);
		break;

	case PBS_BATCH_SelectJobs:
	case PBS_BATCH_SelStat:
		CLEAR_HEAD(request->rq_ind.rq_select.rq_selattr);
		CLEAR_HEAD(request->rq_ind.rq_select.rq_rtnattr);
		rc = decode_DIS_svrattrl(sfds,
					 &request->rq_ind.rq_select.rq_selattr);
		rc = decode_DIS_svrattrl(sfds,
					 &request->rq_ind.rq_select.rq_rtnattr);
		break;

	case PBS_BATCH_StatusNode:
	case PBS_BATCH_StatusResv:
	case PBS_BATCH_StatusQue:
	case PBS_BATCH_StatusSvr:
	case PBS_BATCH_StatusSched:
	case PBS_BATCH_StatusRsc:
	case PBS_BATCH_StatusHook:
		rc = decode_DIS_Status(sfds, request);
		break;

	case PBS_BATCH_TrackJob:
		rc = decode_DIS_TrackJob(sfds, request);
		break;

	case PBS_BATCH_Rescq:
	case PBS_BATCH_ReserveResc:
	case PBS_BATCH_ReleaseResc:
		rc = decode_DIS_Rescl(sfds, request);
		break;

	case PBS_BATCH_RegistDep:
		rc = decode_DIS_Register(sfds, request);
		break;

	case PBS_BATCH_ModifyResv:
		decode_DIS_ModifyResv(sfds, request);
		break;

	case PBS_BATCH_PreemptJobs:
		decode_DIS_PreemptJobs(sfds, request);
		break;

#else /* yes PBS_MOM */

	case PBS_BATCH_CopyHookFile:
		rc = decode_DIS_CopyHookFile(sfds, request);
		break;

	case PBS_BATCH_DelHookFile:
		rc = decode_DIS_DelHookFile(sfds, request);
		break;

	case PBS_BATCH_CopyFiles:
	case PBS_BATCH_DelFiles:
		rc = decode_DIS_CopyFiles(sfds, request);
		break;

	case PBS_BATCH_CopyFiles_Cred:
	case PBS_BATCH_DelFiles_Cred:
		rc = decode_DIS_CopyFiles_Cred(sfds, request);
		break;
	case PBS_BATCH_Cred:
		rc = decode_DIS_Cred(sfds, request);
		break;

#endif /* PBS_MOM */

	default:
		sprintf(log_buffer, "%s: %d from %s", msg_nosupport,
			request->rq_type, request->rq_user);
		log_event(PBSEVENT_DEBUG, PBS_EVENTCLASS_REQUEST, LOG_DEBUG,
			  "?", log_buffer);
		rc = PBSE_UNKREQ;
		break;
	}

	if (rc == 0) { /* Decode the Request Extension, if present */
		rc = decode_DIS_ReqExtend(sfds, request);
		if (rc != 0) {
			(void) sprintf(log_buffer,
				       "Request type: %d Req Extension bad, dis error %d", request->rq_type, rc);
			log_event(PBSEVENT_DEBUG, PBS_EVENTCLASS_REQUEST,
				  LOG_DEBUG, "?", log_buffer);
			rc = PBSE_DISPROTO;
		}
	} else if (rc != PBSE_UNKREQ) {
		(void) sprintf(log_buffer,
			       "Req Body bad, dis error %d, type %d",
			       rc, request->rq_type);
		log_event(PBSEVENT_DEBUG, PBS_EVENTCLASS_REQUEST,
			  LOG_DEBUG, "?", log_buffer);
		rc = PBSE_DISPROTO;
	}

	return (rc);
}

void
req_authenticate(conn_t *conn, struct batch_request *request)
{
	auth_def_t *authdef = NULL;
	auth_def_t *encryptdef = NULL;
	conn_t *cp = NULL;

	if (!is_string_in_arr(pbs_conf.supported_auth_methods, request->rq_ind.rq_auth.rq_auth_method)) {
		req_reject(PBSE_NOSUP, 0, request);
		close_client(conn->cn_sock);
		return;
	}

	if (request->rq_ind.rq_auth.rq_encrypt_method[0] != '\0') {
		encryptdef = get_auth(request->rq_ind.rq_auth.rq_encrypt_method);
		if (encryptdef == NULL || encryptdef->encrypt_data == NULL || encryptdef->decrypt_data == NULL) {
			req_reject(PBSE_NOSUP, 0, request);
			close_client(conn->cn_sock);
			return;
		}
	}

	if (strcmp(request->rq_ind.rq_auth.rq_auth_method, AUTH_RESVPORT_NAME) != 0) {
		authdef = get_auth(request->rq_ind.rq_auth.rq_auth_method);
		if (authdef == NULL) {
			req_reject(PBSE_NOSUP, 0, request);
			close_client(conn->cn_sock);
			return;
		}
		cp = conn;
	} else {
		/* ensure resvport auth request is coming from priv port */
		if ((conn->cn_authen & PBS_NET_CONN_FROM_PRIVIL) == 0) {
			req_reject(PBSE_BADCRED, 0, request);
			close_client(conn->cn_sock);
			return;
		}
		cp = (conn_t *) GET_NEXT(svr_allconns);
		for (; cp != NULL; cp = GET_NEXT(cp->cn_link)) {
			if (request->rq_ind.rq_auth.rq_port == cp->cn_port && conn->cn_addr == cp->cn_addr) {
				cp->cn_authen |= PBS_NET_CONN_AUTHENTICATED;
				break;
			}
		}
		if (cp == NULL) {
			req_reject(PBSE_BADCRED, 0, request);
			close_client(conn->cn_sock);
			return;
		}
	}

	cp->cn_auth_config = make_auth_config(request->rq_ind.rq_auth.rq_auth_method,
					      request->rq_ind.rq_auth.rq_encrypt_method,
					      pbs_conf.pbs_exec_path,
					      pbs_conf.pbs_home_path,
					      (void *) log_event);
	if (cp->cn_auth_config == NULL) {
		req_reject(PBSE_SYSTEM, 0, request);
		close_client(conn->cn_sock);
		return;
	}

	(void) strcpy(cp->cn_username, request->rq_user);
	(void) strcpy(cp->cn_hostname, request->rq_host);
	cp->cn_timestamp = time_now;

	if (encryptdef != NULL) {
		encryptdef->set_config((const pbs_auth_config_t *) (cp->cn_auth_config));
		transport_chan_set_authdef(cp->cn_sock, encryptdef, FOR_ENCRYPT);
		transport_chan_set_ctx_status(cp->cn_sock, AUTH_STATUS_CTX_ESTABLISHING, FOR_ENCRYPT);
	}

	if (authdef != NULL) {
		if (encryptdef != authdef)
			authdef->set_config((const pbs_auth_config_t *) (cp->cn_auth_config));
		transport_chan_set_authdef(cp->cn_sock, authdef, FOR_AUTH);
		transport_chan_set_ctx_status(cp->cn_sock, AUTH_STATUS_CTX_ESTABLISHING, FOR_AUTH);
	}
	if (strcmp(request->rq_ind.rq_auth.rq_auth_method, AUTH_RESVPORT_NAME) == 0) {
		transport_chan_set_ctx_status(cp->cn_sock, AUTH_STATUS_CTX_READY, FOR_AUTH);
	}
	reply_ack(request);
}

/**
 * @brief
 * 		close_client - close a connection to a client, also "inactivate"
 *		  any outstanding batch requests on that connection.
 *
 * @param[in]	sfds	- connection socket
 */

void
close_client(int sfds)
{
	struct batch_request *preq;

	close_conn(sfds); /* close the connection */
	preq = (struct batch_request *) GET_NEXT(svr_requests);
	while (preq) { /* list of outstanding requests */
		if (preq->rq_conn == sfds)
			preq->rq_conn = -1;
		if (preq->rq_orgconn == sfds)
			preq->rq_orgconn = -1;
		preq = (struct batch_request *) GET_NEXT(preq->rq_link);
	}
}

/**
 * @brief
 * 		alloc_br - allocate and clear a batch_request structure
 *
 * @param[in]	type	- type of request
 *
 * @return	batch_request *
 * @retval	NULL	- error
 */

struct batch_request *
alloc_br(int type)
{
	struct batch_request *req;

	req = (struct batch_request *) malloc(sizeof(struct batch_request));
	if (req == NULL)
		log_err(errno, "alloc_br", msg_err_malloc);
	else {
		memset((void *) req, (int) 0, sizeof(struct batch_request));
		req->rq_type = type;
		CLEAR_LINK(req->rq_link);
		req->rq_conn = -1;    /* indicate not connected */
		req->rq_orgconn = -1; /* indicate not connected */
		req->rq_time = time_now;
		req->tpp_ack = 1;	  /* enable acks to be passed by tpp by default */
		req->prot = PROT_TCP;	  /* not tpp by default */
		req->tppcmd_msgid = NULL; /* NULL msgid to boot */
		req->rq_reply.brp_choice = BATCH_REPLY_CHOICE_NULL;
		append_link(&svr_requests, &req->rq_link, req);
	}
	return (req);
}

/**
 * @brief
 *		Read a bunch of strings into a NULL terminated array.
 *		The strings are regular null terminated char arrays
 *		and the string array is NULL terminated.
 *
 *		Pass in array location to hold the allocated array
 *		and return an error value if there is a problem.  If
 *		an error does occur, arrloc is not changed.
 *
 * @param[in]	stream	- socket where you reads the request.
 * @param[out]	arrloc	- NULL terminated array where strings are stored.
 *
 * @return	error code
 */
int
read_carray(int stream, char ***arrloc)
{
	int i, num, ret;
	char *cp, **carr;

	if (arrloc == NULL)
		return PBSE_INTERNAL;

	num = 4; /* keep track of the number of array slots */
	carr = (char **) calloc(sizeof(char **), num);
	if (carr == NULL)
		return PBSE_SYSTEM;

	for (i = 0;; i++) {
		cp = disrst(stream, &ret);
		if ((cp == NULL) || (ret != DIS_SUCCESS)) {
			free_string_array(carr);
			if (cp != NULL)
				free(cp);
			return PBSE_SYSTEM;
		}
		if (*cp == '\0') {
			free(cp);
			break;
		}
		if (i == num - 1) {
			char **hold;

			hold = (char **) realloc(carr,
						 num * 2 * sizeof(char **));
			if (hold == NULL) {
				free_string_array(carr);
				free(cp);
				return PBSE_SYSTEM;
			}
			carr = hold;

			/* zero the last half of the now doubled carr */
			memset(&carr[num], 0, num * sizeof(char **));
			num *= 2;
		}
		carr[i] = cp;
	}
	carr[i] = NULL;
	*arrloc = carr;
	return ret;
}

/**
 * @brief
 *		Read a python spawn request off the wire.
 *		Each of the argv and envp arrays is sent by writing a counted
 *		string followed by a zero length string ("").
 *
 * @param[in]	sock	- socket where you reads the request.
 * @param[in]	preq	- the batch_request structure to free up.
 */
int
decode_DIS_PySpawn(int sock, struct batch_request *preq)
{
	int rc;

	rc = disrfst(sock, sizeof(preq->rq_ind.rq_py_spawn.rq_jid),
		     preq->rq_ind.rq_py_spawn.rq_jid);
	if (rc)
		return rc;

	rc = read_carray(sock, &preq->rq_ind.rq_py_spawn.rq_argv);
	if (rc)
		return rc;

	rc = read_carray(sock, &preq->rq_ind.rq_py_spawn.rq_envp);
	if (rc)
		return rc;

	return rc;
}

/**
 * @brief
 *		Read a release nodes from job request off the wire.
 *
 * @param[in]	sock	- socket where you reads the request.
 * @param[in]	preq	- the batch_request structure containing the request details.
 *
 * @return int
 *
 * @retval	0	- if successful
 * @retval	!= 0	- if not successful (an error encountered along the way)
 */
int
decode_DIS_RelnodesJob(int sock, struct batch_request *preq)
{
	int rc;

	preq->rq_ind.rq_relnodes.rq_node_list = NULL;

	rc = disrfst(sock, PBS_MAXSVRJOBID + 1, preq->rq_ind.rq_relnodes.rq_jid);
	if (rc)
		return rc;

	preq->rq_ind.rq_relnodes.rq_node_list = disrst(sock, &rc);
	return rc;
}

#ifndef PBS_MOM
/**
 * @brief
 * 		free_rescrq - free resource queue.
 *
 * @param[in,out]	pq	- resource queue
 */
static void
free_rescrq(struct rq_rescq *pq)
{
	int i;

	i = pq->rq_num;
	while (i--) {
		if (*(pq->rq_list + i))
			(void)free(*(pq->rq_list + i));
	}
	if (pq->rq_list)
		(void)free(pq->rq_list);
}
#endif

/**
 * @brief
 * 		Free space allocated to a batch_request structure
 *		including any sub-structures
 *
 * @param[in]	preq - the batch_request structure to free up.
 */

void
free_br(struct batch_request *preq)
{
	delete_link(&preq->rq_link);
	reply_free(&preq->rq_reply);

	if (preq->rq_parentbr) {
		/*
		 * have a parent who has the original info, so we cannot
		 * free any data malloc-ed outside of the basic structure;
		 * decrement the reference count in the parent and when it
		 * goes to zero,  reply_send() it
		 */
		if (preq->rq_parentbr->rq_refct > 0) {
			if (--preq->rq_parentbr->rq_refct == 0)
				reply_send(preq->rq_parentbr);
		}

		if (preq->tppcmd_msgid)
			free(preq->tppcmd_msgid);

		(void) free(preq);
		return;
	}

	/*
	 * IMPORTANT - free any data that is malloc-ed outside of the
	 * basic batch_request structure below here so it is not freed
	 * when a copy of the structure (for a Array subjob) is freed
	 */
	if (preq->rq_extend)
		(void) free(preq->rq_extend);

	switch (preq->rq_type) {
	case PBS_BATCH_QueueJob:
		free_attrlist(&preq->rq_ind.rq_queuejob.rq_attr);
		break;
	case PBS_BATCH_JobCred:
		if (preq->rq_ind.rq_jobcred.rq_data)
			(void) free(preq->rq_ind.rq_jobcred.rq_data);
		break;
	case PBS_BATCH_UserCred:
		if (preq->rq_ind.rq_usercred.rq_data)
			(void) free(preq->rq_ind.rq_usercred.rq_data);
		break;
	case PBS_BATCH_jobscript:
		if (preq->rq_ind.rq_jobfile.rq_data)
			(void) free(preq->rq_ind.rq_jobfile.rq_data);
		break;
	case PBS_BATCH_CopyHookFile:
		if (preq->rq_ind.rq_hookfile.rq_data)
			(void) free(preq->rq_ind.rq_hookfile.rq_data);
		break;
	case PBS_BATCH_HoldJob:
		freebr_manage(&preq->rq_ind.rq_hold.rq_orig);
		break;
	case PBS_BATCH_MessJob:
		if (preq->rq_ind.rq_message.rq_text)
			(void) free(preq->rq_ind.rq_message.rq_text);
		break;
	case PBS_BATCH_RelnodesJob:
		if (preq->rq_ind.rq_relnodes.rq_node_list)
			(void) free(preq->rq_ind.rq_relnodes.rq_node_list);
		break;
	case PBS_BATCH_PySpawn:
		free_string_array(preq->rq_ind.rq_py_spawn.rq_argv);
		free_string_array(preq->rq_ind.rq_py_spawn.rq_envp);
		break;
	case PBS_BATCH_ModifyJob:
	case PBS_BATCH_ModifyResv:
	case PBS_BATCH_ModifyJob_Async:
		freebr_manage(&preq->rq_ind.rq_modify);
		break;

	case PBS_BATCH_RunJob:
	case PBS_BATCH_AsyrunJob:
	case PBS_BATCH_StageIn:
	case PBS_BATCH_ConfirmResv:
		if (preq->rq_ind.rq_run.rq_destin)
			(void) free(preq->rq_ind.rq_run.rq_destin);
		break;
	case PBS_BATCH_StatusJob:
	case PBS_BATCH_StatusQue:
	case PBS_BATCH_StatusNode:
	case PBS_BATCH_StatusSvr:
	case PBS_BATCH_StatusSched:
	case PBS_BATCH_StatusHook:
	case PBS_BATCH_StatusRsc:
	case PBS_BATCH_StatusResv:
		if (preq->rq_ind.rq_status.rq_id)
			free(preq->rq_ind.rq_status.rq_id);
		free_attrlist(&preq->rq_ind.rq_status.rq_attr);
		break;
	case PBS_BATCH_CopyFiles:
	case PBS_BATCH_DelFiles:
		freebr_cpyfile(&preq->rq_ind.rq_cpyfile);
		break;
	case PBS_BATCH_CopyFiles_Cred:
	case PBS_BATCH_DelFiles_Cred:
		freebr_cpyfile_cred(&preq->rq_ind.rq_cpyfile_cred);
		break;
	case PBS_BATCH_MvJobFile:
		if (preq->rq_ind.rq_jobfile.rq_data)
			free(preq->rq_ind.rq_jobfile.rq_data);
		break;
	case PBS_BATCH_Cred:
		if (preq->rq_ind.rq_cred.rq_cred_data)
			free(preq->rq_ind.rq_cred.rq_cred_data);
		break;

#ifndef PBS_MOM /* Server Only */

	case PBS_BATCH_SubmitResv:
		free_attrlist(&preq->rq_ind.rq_queuejob.rq_attr);
		break;
	case PBS_BATCH_Manager:
		freebr_manage(&preq->rq_ind.rq_manager);
		break;
	case PBS_BATCH_ReleaseJob:
		freebr_manage(&preq->rq_ind.rq_release);
		break;
	case PBS_BATCH_Rescq:
	case PBS_BATCH_ReserveResc:
	case PBS_BATCH_ReleaseResc:
		free_rescrq(&preq->rq_ind.rq_rescq);
		break;
	case PBS_BATCH_DefSchReply:
		free(preq->rq_ind.rq_defrpy.rq_id);
		free(preq->rq_ind.rq_defrpy.rq_txt);
		break;
	case PBS_BATCH_SelectJobs:
	case PBS_BATCH_SelStat:
		free_attrlist(&preq->rq_ind.rq_select.rq_selattr);
		free_attrlist(&preq->rq_ind.rq_select.rq_rtnattr);
		break;
	case PBS_BATCH_PreemptJobs:
		free(preq->rq_ind.rq_preempt.ppj_list);
		free(preq->rq_reply.brp_un.brp_preempt_jobs.ppj_list);
		break;
#endif /* PBS_MOM */
	}
	if (preq->tppcmd_msgid)
		free(preq->tppcmd_msgid);
	(void) free(preq);
}

/**
 * @brief
 * 		it is a wrapper function of free_attrlist()
 *
 * @param[in]	pmgr - request manage structure.
 */
void
freebr_manage(struct rq_manage *pmgr)
{
	free_attrlist(&pmgr->rq_attr);
}

/**
 * @brief
 * 		remove all the rqfpair and free their memory
 *
 * @param[in]	pcf - rq_cpyfile structure on which rq_pairs needs to be freed.
 */
void
freebr_cpyfile(struct rq_cpyfile *pcf)
{
	struct rqfpair *ppair;

	while ((ppair = (struct rqfpair *) GET_NEXT(pcf->rq_pair)) != NULL) {
		delete_link(&ppair->fp_link);
		if (ppair->fp_local)
			(void) free(ppair->fp_local);
		if (ppair->fp_rmt)
			(void) free(ppair->fp_rmt);
		(void) free(ppair);
	}
}

/**
 * @brief
 * 		remove list of rqfpair along with encrpyted credential.
 *
 * @param[in]	pcfc - rq_cpyfile_cred structure
 */
void
freebr_cpyfile_cred(struct rq_cpyfile_cred *pcfc)
{
	struct rqfpair *ppair;

	while ((ppair = (struct rqfpair *) GET_NEXT(pcfc->rq_copyfile.rq_pair)) != NULL) {
		delete_link(&ppair->fp_link);
		if (ppair->fp_local)
			(void) free(ppair->fp_local);
		if (ppair->fp_rmt)
			(void) free(ppair->fp_rmt);
		(void) free(ppair);
	}
	if (pcfc->rq_pcred)
		free(pcfc->rq_pcred);
}

/**
 * @brief
 * 		parse_servername - parse a server/vnode name in the form:
 *		[(]name[:service_port][:resc=value[:...]][+name...]
 *		from exec_vnode or from exec_hostname
 *		name[:service_port]/NUMBER[*NUMBER][+...]
 *		or basic servername:port string
 *
 *		Returns ptr to the node name as the function value and the service_port
 *		number (int) into service if :port is found, otherwise port is unchanged
 *		host name is also terminated by a ':', '+' or '/' in string
 *
 * @param[in]	name	- server/node/exec_vnode string
 * @param[out]	service	-  RETURN: service_port if :port
 *
 * @return	 ptr to the node name
 *
 * @par MT-safe: No
 */

char *
parse_servername(char *name, unsigned int *service)
{
	static char buf[PBS_MAXSERVERNAME + PBS_MAXPORTNUM + 2];
	int i = 0;
	char *pc;

	if ((name == NULL) || (*name == '\0'))
		return NULL;
	if (*name == '(') /* skip leading open paren found in exec_vnode */
		name++;

	/* look for a ':', '+' or '/' in the string */

	pc = name;
	while (*pc && (i < PBS_MAXSERVERNAME + PBS_MAXPORTNUM + 2)) {
		if ((*pc == '+') || (*pc == '/')) {
			break;
		} else if (*pc == ':') {
			if (isdigit((int) *(pc + 1)) && (service != NULL))
				*service = (unsigned int) atoi(pc + 1);
			break;
		} else {
			buf[i++] = *pc++;
		}
	}
	buf[i] = '\0';
	return (buf);
}

/**
 * @brief
 * 		Obtain the name and port of the server as defined by pbs_conf
 *
 * @param[out] port - Passed through to parse_servername(), not modified here.
 *
 * @return char *
 * @return NULL - failure
 * @retval !NULL - pointer to server name
 */
char *
get_servername(unsigned int *port)
{
	char *name = NULL;

	if (pbs_conf.pbs_primary)
		name = parse_servername(pbs_conf.pbs_primary, port);
	else if (pbs_conf.pbs_server_host_name)
		name = parse_servername(pbs_conf.pbs_server_host_name, port);
	else
		name = parse_servername(pbs_conf.pbs_server_name, port);

	return name;
}
