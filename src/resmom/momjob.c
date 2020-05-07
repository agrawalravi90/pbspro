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

#include <assert.h>
#include <errno.h>
#include <signal.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>

#include "avltree.h"
#include "job.h"
#include "libpbs.h"
#include "list_link.h"
#include "log.h"
#include "momjob.h"
#include "pbs_error.h"
#include "pbs_reliable.h"

#if defined(PBS_SECURITY) && (PBS_SECURITY == KRB5)
#include "renew_creds.h"
#endif

#include "mom_func.h"

extern void rmtmpdir(char *);
void nodes_free(job *);
extern char *std_file_name(job *pjob, enum job_file which, int *keeping);
extern char *path_checkpoint;

/**
 * @brief
 * 		free up the tasks from the list of tasks associated with particular job, delete links and close connection.
 *
 * @param[in]	pj - pointer to job structure
 *
 * @return void
 */
void
tasks_free(job *pj)
{
	pbs_task *tp = (pbs_task *) GET_NEXT(pj->ji_tasks);
	obitent *op;
	infoent *ip;
	int i;

	while (tp) {
		op = (obitent *) GET_NEXT(tp->ti_obits);
		while (op) {
			delete_link(&op->oe_next);
			free(op);
			op = (obitent *) GET_NEXT(tp->ti_obits);
		}

		ip = (infoent *) GET_NEXT(tp->ti_info);
		while (ip) {
			delete_link(&ip->ie_next);
			free(ip->ie_name);
			free(ip->ie_info);
			free(ip);
			ip = (infoent *) GET_NEXT(tp->ti_info);
		}

		if (tp->ti_tmfd != NULL) {
			for (i = 0; i < tp->ti_tmnum; i++)
				close_conn(tp->ti_tmfd[i]);
			free(tp->ti_tmfd);
		}
		delete_link(&tp->ti_jobtask);
		free(tp);
		tp = (pbs_task *) GET_NEXT(pj->ji_tasks);
	}
}

/**
 * @brief
 * 		job_alloc - allocate space for a job structure and initialize working
 *				attribute to "unset"
 *
 * @return	pointer to structure or null is space not available.
 */

job *
job_alloc(void)
{
	job *pj;

	pj = (job *) malloc(sizeof(job));
	if (pj == NULL) {
		log_err(errno, "job_alloc", "no memory");
		return NULL;
	}
	memset((char *) pj, (int) 0, (size_t) sizeof(job));

	CLEAR_LINK(pj->ji_alljobs);
	CLEAR_LINK(pj->ji_jobque);
	CLEAR_LINK(pj->ji_unlicjobs);

	pj->ji_rerun_preq = NULL;

	CLEAR_HEAD(pj->ji_tasks);
	CLEAR_HEAD(pj->ji_failed_node_list);
	CLEAR_HEAD(pj->ji_node_list);
	pj->ji_taskid = TM_INIT_TASK;
	pj->ji_numnodes = 0;
	pj->ji_numrescs = 0;
	pj->ji_numvnod = 0;
	pj->ji_num_assn_vnodes = 0;
	pj->ji_hosts = NULL;
	pj->ji_vnods = NULL;
	pj->ji_assn_vnodes = NULL;
	pj->ji_resources = NULL;
	pj->ji_obit = TM_NULL_EVENT;
	pj->ji_postevent = TM_NULL_EVENT;
	pj->ji_preq = NULL;
	pj->ji_nodekill = TM_ERROR_NODE;
	pj->ji_flags = 0;
	pj->ji_jsmpipe = -1;
	pj->ji_mjspipe = -1;
	pj->ji_jsmpipe2 = -1;
	pj->ji_mjspipe2 = -1;
	pj->ji_child2parent_job_update_pipe = -1;
	pj->ji_parent2child_job_update_pipe = -1;
	pj->ji_parent2child_job_update_status_pipe = -1;
	pj->ji_parent2child_moms_status_pipe = -1;
	pj->ji_updated = 0;
	pj->ji_hook_running_bg_on = BG_NONE;
#ifdef WIN32
	pj->ji_hJob = NULL;
	pj->ji_user = NULL;
	pj->ji_grpcache = NULL;
#endif
	pj->ji_stdout = 0;
	pj->ji_stderr = 0;
	pj->ji_setup = NULL;
	pj->ji_qs.ji_jsversion = JSVERSION;
	pj->ji_momhandle = -1;		/* mark mom connection invalid */
	pj->ji_mom_prot = PROT_INVALID; /* invalid protocol type */
	pj->ji_momsubt = 0;

	/* set the working attributes to "unspecified" */

	job_init_wattr(pj);

	return (pj);
}

/**
 * @brief
 * 		job_free - free job structure and its various sub-structures
 *
 * @param[in]	pj - pointer to job structure
 *
 * @return	void
 */
void
job_free(job *pj)
{
	int i;

#ifdef WIN32
	if (pj->ji_wattr[(int) JOB_ATR_altid].at_flags & ATR_VFLAG_SET) {
		char *p;

		p = strstr(pj->ji_wattr[(int) JOB_ATR_altid].at_val.at_str,
			   "HomeDirectory=");
		if (p) {
			struct passwd *pwdp = NULL;

			if ((pj->ji_wattr[JOB_ATR_euser].at_val.at_str) &&
			    (pwdp = getpwnam(pj->ji_wattr[JOB_ATR_euser].at_val.at_str))) {
				if (pwdp->pw_userlogin != INVALID_HANDLE_VALUE) {
					if (impersonate_user(pwdp->pw_userlogin) == 0)
						return;
				}
				/* p+14 is the string after HomeDirectory= */
				unmap_unc_path(p + 14);
				(void) revert_impersonated_user();
			}
			unmap_unc_path(p + 14); /* also unmap under Admin to be sure */
		}
	}
#endif

	/* remove any malloc working attribute space */
	for (i = 0; i < (int) JOB_ATR_LAST; i++) {
		job_attr_def[i].at_free(&pj->ji_wattr[i]);
	}

	if (pj->ji_grpcache)
		(void) free(pj->ji_grpcache);

	assert(pj->ji_preq == NULL);
	nodes_free(pj);
	tasks_free(pj);
	if (pj->ji_resources) {
		for (i = 0; i < pj->ji_numrescs; i++) {
			free(pj->ji_resources[i].nodehost);
			pj->ji_resources[i].nodehost = NULL;
			if ((pj->ji_resources[i].nr_used.at_flags & ATR_VFLAG_SET) != 0) {
				job_attr_def[(int) JOB_ATR_resc_used].at_free(&pj->ji_resources[i].nr_used);
			}
		}
		pj->ji_numrescs = 0;
		free(pj->ji_resources);
		pj->ji_resources = NULL;
	}

	reliable_job_node_free(&pj->ji_failed_node_list);
	reliable_job_node_free(&pj->ji_node_list);

	/*
	 ** This gets rid of any dependent job structure(s) from ji_setup.
	 */
	if (job_free_extra != NULL)
		job_free_extra(pj);

#ifdef WIN32
	if (pj->ji_hJob) {
		CloseHandle(pj->ji_hJob);
		pj->ji_hJob = NULL;
	}
#endif

	/* if a subjob (of a Array Job), do not free certain items */
	/* which are malloced and shared with the parent Array Job */
	/* They will be freed when the parent is removed           */

	pj->ji_qs.ji_jobid[0] = 'X'; /* as a "freed" marker */
	free(pj);		     /* now free the main structure */
}

/**
 * @brief
 * 		job_purge - purge job from system
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
job_purge(job *pjob)
{
	extern char *msg_err_purgejob;
	char namebuf[MAXPATHLEN + 1] = {'\0'};
	int keeping = 0;
	attribute *jrpattr = NULL;

#ifndef WIN32
	pid_t pid = -1;
	int child_process = 0;
#endif

	if (pjob->ji_rerun_preq != NULL) {
		log_joberr(PBSE_INTERNAL, __func__, "rerun request outstanding", pjob->ji_qs.ji_jobid);
		reply_text(pjob->ji_rerun_preq, PBSE_INTERNAL, "job rerun");
		pjob->ji_rerun_preq = NULL;
	}

	delete_link(&pjob->ji_jobque);
	delete_link(&pjob->ji_alljobs);
	delete_link(&pjob->ji_unlicjobs);

	if (pjob->ji_preq != NULL) {
		log_joberr(PBSE_INTERNAL, __func__, "request outstanding", pjob->ji_qs.ji_jobid);
		reply_text(pjob->ji_preq, PBSE_INTERNAL, "job deleted");
		pjob->ji_preq = NULL;
	}
#ifndef WIN32

	if (pjob->ji_momsubt != 0) { /* child running */
		kill(pjob->ji_momsubt, SIGKILL);
		pjob->ji_momsubt = 0;
	}
	/* if open, close pipes to/from Mom starter process */
	if (pjob->ji_jsmpipe != -1) {
		conn_t *connection = NULL;

		if ((pjob->ji_wattr[(int) JOB_ATR_session_id].at_flags & ATR_VFLAG_SET) == 0 &&
		    !(pjob->ji_wattr[(int) JOB_ATR_session_id].at_val.at_long) &&
		    (connection = get_conn(pjob->ji_jsmpipe)) != NULL) {
			/*
			 * If session id for the job is not set, retain pjob->ji_jsmpipe.
			 * Set cn_data to NULL so that we can kill the process when
			 * record_finish_exec is called.
			 */
			connection->cn_data = NULL;
		} else
			close_conn(pjob->ji_jsmpipe);
	}
	if (pjob->ji_mjspipe != -1) {
		close(pjob->ji_mjspipe);
	}

	/* if open, close 2nd pipes to/from Mom starter process */
	if (pjob->ji_jsmpipe2 != -1) {
		close_conn(pjob->ji_jsmpipe2);
	}
	if (pjob->ji_mjspipe2 != -1) {
		close(pjob->ji_mjspipe2);
	}

	/* if open, close 3rd pipes to/from Mom starter process */
	if (pjob->ji_child2parent_job_update_pipe != -1) {
		close_conn(pjob->ji_child2parent_job_update_pipe);
	}
	if (pjob->ji_parent2child_job_update_pipe != -1) {
		close(pjob->ji_parent2child_job_update_pipe);
	}

	/* if open, close 4th pipes to/from Mom starter process */
	if (pjob->ji_parent2child_job_update_status_pipe != -1) {
		close(pjob->ji_parent2child_job_update_status_pipe);
	}

	/* if open, close 5th pipes to/from Mom starter process */
	if (pjob->ji_parent2child_moms_status_pipe != -1) {
		close(pjob->ji_parent2child_moms_status_pipe);
	}
#endif

#ifndef WIN32
	/* on the mom end, perform file-system related cleanup in a forked process
	 * only if job is executed successfully with exit status 0(JOB_EXEC_OK)
	 */
	if (pjob->ji_qs.ji_un.ji_momt.ji_exitstat == JOB_EXEC_OK) {
		child_process = 1;
		pid = fork();
		if (pid > 0) {
#if defined(PBS_SECURITY) && (PBS_SECURITY == KRB5)
			delete_cred(pjob->ji_qs.ji_jobid);
#endif

			/* parent mom */
			job_free(pjob);
			return;
		}
	}
	/* Parent Mom process will continue the job cleanup itself, if call to fork is failed */
#endif
	/* delete script file */
	del_job_related_file(pjob, JOB_SCRIPT_SUFFIX);

	if (pjob->ji_preq != NULL) {
		req_reject(PBSE_MOMREJECT, 0, pjob->ji_preq);
		pjob->ji_preq = NULL;
	}

	del_job_dirs(pjob);

	del_chkpt_files(pjob);

	jrpattr = &pjob->ji_wattr[JOB_ATR_remove];
	/* remove stdout/err files if remove_files is set. */
	if ((jrpattr->at_flags & ATR_VFLAG_SET) && (pjob->ji_qs.ji_un.ji_momt.ji_exitstat == JOB_EXEC_OK)) {
		if (strchr(jrpattr->at_val.at_str, 'o')) {
			strcpy(namebuf, std_file_name(pjob, StdOut, &keeping));
			if (*namebuf && (unlink(namebuf) < 0))
				if (errno != ENOENT)
					log_err(errno, __func__, msg_err_purgejob);
		}
		if (strchr(jrpattr->at_val.at_str, 'e')) {
			strcpy(namebuf, std_file_name(pjob, StdErr, &keeping));
			if (*namebuf && (unlink(namebuf) < 0))
				if (errno != ENOENT)
					log_err(errno, __func__, msg_err_purgejob);
		}
	}

#ifdef WIN32
	/* following introduced by fix to BZ 6363 for executing scripts */
	/* directly on the command line */
	strcpy(namebuf, path_jobs); /* delete any *.BAT file */
	if (*pjob->ji_qs.ji_fileprefix != '\0')
		strcat(namebuf, pjob->ji_qs.ji_fileprefix);
	else
		strcat(namebuf, pjob->ji_qs.ji_jobid);
	strcat(namebuf, ".BAT");

	if (unlink(namebuf) < 0) {
		if (errno != ENOENT)
			log_err(errno, __func__, msg_err_purgejob);
	}
#endif

	/* delete job file */
	del_job_related_file(pjob, JOB_FILE_SUFFIX);

#if defined(PBS_SECURITY) && (PBS_SECURITY == KRB5)
	delete_cred(pjob->ji_qs.ji_jobid);
#endif

	del_job_related_file(pjob, JOB_CRED_SUFFIX);

	/* Clearing purge job info from svr_newjobs list */
	if (pjob == GET_NEXT(svr_newjobs))
		delete_link(&pjob->ji_alljobs);

	job_free(pjob);

#ifndef WIN32
	if (child_process && pid == 0) {
		/* I am child of the forked process. Deleted all the
		 * particular job related files, thus exiting.
		 */
		exit(0);
	}
#endif
}

/**
 * @brief
 * 		find_job() - find job by jobid
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

job *
find_job(char *jobid)
{
	char *at;
	job *pj = NULL;
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

	pj = (job *) GET_NEXT(svr_alljobs);
	while (pj != NULL) {
		if (!strncasecmp(jobid, pj->ji_qs.ji_jobid, sizeof(pj->ji_qs.ji_jobid)))
			break;
		pj = (job *) GET_NEXT(pj->ji_alljobs);
	}

	return pj; /* may be a null pointer */
}

/**
 * @brief	Convenience function to delete directories associated with a job being purged
 *
 * @param[in]	pjob - the job being purged
 *
 * @return	void
 */
void
del_job_dirs(job *pjob)
{
	char namebuf[MAXPATHLEN + 1] = {'\0'};

	strcpy(namebuf, path_jobs); /* job directory path */
	if (*pjob->ji_qs.ji_fileprefix != '\0')
		strcat(namebuf, pjob->ji_qs.ji_fileprefix);
	else
		strcat(namebuf, pjob->ji_qs.ji_jobid);
	strcat(namebuf, JOB_TASKDIR_SUFFIX);
	remtree(namebuf);

	rmtmpdir(pjob->ji_qs.ji_jobid); /* remove tmpdir */

	/* remove the staging and execution directory when sandbox=PRIVATE
	 ** and there are no stage-out errors
	 */
	if ((pjob->ji_wattr[(int) JOB_ATR_sandbox].at_flags & ATR_VFLAG_SET) &&
	    (strcasecmp(pjob->ji_wattr[JOB_ATR_sandbox].at_val.at_str, "PRIVATE") == 0)) {
		if (!(pjob->ji_qs.ji_svrflags & JOB_SVFLG_StgoFal)) {
			if (pjob->ji_grpcache != NULL)
				rmjobdir(pjob->ji_qs.ji_jobid,
					 jobdirname(pjob->ji_qs.ji_jobid, pjob->ji_grpcache->gc_homedir),
					 pjob->ji_grpcache->gc_uid,
					 pjob->ji_grpcache->gc_gid);
			else
				rmjobdir(pjob->ji_qs.ji_jobid, jobdirname(pjob->ji_qs.ji_jobid, NULL), 0, 0);
		}
	}
}

/**
 * @brief	Convenience function to delete checkpoint files for a job being purged
 *
 * @param[in]	pjob - job being purged
 *
 * @return void
 */
void
del_chkpt_files(job *pjob)
{
	char namebuf[MAXPATHLEN + 1] = {'\0'};

	if (path_checkpoint != NULL) { /* delete checkpoint files */
		(void) strcpy(namebuf, path_checkpoint);
		if (*pjob->ji_qs.ji_fileprefix != '\0')
			(void) strcat(namebuf, pjob->ji_qs.ji_fileprefix);
		else
			(void) strcat(namebuf, pjob->ji_qs.ji_jobid);
		(void) strcat(namebuf, JOB_CKPT_SUFFIX);
		(void) remtree(namebuf);
		(void) strcat(namebuf, ".old");
		(void) remtree(namebuf);
	}
}

/**
 * @brief
 * 		direct_write_requested - checks whether direct_write is requested by the job.
 *
 * @param[in]	pjob	- pointer to job structure.
 *
 * @return	bool
 * @retval 1 : direct write is requested by the job.
 * @retval 0 : direct write is not requested by the job.
 */
int
direct_write_requested(job *pjob)
{
	char *pj_attrk = NULL;
	if ((pjob->ji_wattr[(int) JOB_ATR_keep].at_flags & ATR_VFLAG_SET)) {
		pj_attrk = pjob->ji_wattr[(int) JOB_ATR_keep].at_val.at_str;
		if (strchr(pj_attrk, 'd') && (strchr(pj_attrk, 'o') || (strchr(pj_attrk, 'e'))))
			return 1;
	}
	return 0;
}

/**
 * @brief
 * 	Returns 1 if job 'job' should remain running in spite of node failures.
 * @param[in]	pjob	- job being queried
 *
 * @return int
 * @retval	1 - if true
 * @retval	0 - if false or 'tolerate_node_failures' attribute is unset
 */
int
do_tolerate_node_failures(job *pjob)
{

	if (pjob == NULL)
		return (0);

#if MOM_ALPS
	/* not currently supported on the Crays */
	return (0);
#endif

	if ((pjob->ji_wattr[(int) JOB_ATR_tolerate_node_failures].at_flags & ATR_VFLAG_SET) &&
	    ((strcmp(pjob->ji_wattr[(int) JOB_ATR_tolerate_node_failures].at_val.at_str, TOLERATE_NODE_FAILURES_ALL) == 0) ||
	     ((strcmp(pjob->ji_wattr[(int) JOB_ATR_tolerate_node_failures].at_val.at_str, TOLERATE_NODE_FAILURES_JOB_START) == 0) &&
	      pjob->ji_qs.ji_substate != JOB_SUBSTATE_RUNNING))) {
		return (1);
	}
	return (0);
}
