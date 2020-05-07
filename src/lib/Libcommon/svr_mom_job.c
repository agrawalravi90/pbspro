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
#include <assert.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "batch_request.h"
#include "job.h"
#include "list_link.h"
#include "log.h"
#include "net_connect.h"
#include "server_limits.h"
#include "svrfunc.h"
#include "ticket.h"
#include "tpp.h"

extern pbs_list_head svr_newjobs;

#ifdef WIN32
extern int read_cred(svrjob_t *pjob, char **cred, size_t *len);
#endif

/**
 * @brief
 * 		locate_new_job - locate a "new" job which has been set up req_quejob on
 *		the servers new job list.
 *
 * @par Functionality:
 *		This function is used by the sub-requests which make up the global
 *		"Queue Job Request" to locate the job structure.
 *
 *		If the jobid is specified (will be for rdytocommit and commit, but not
 *		for script), we search for a matching jobid.
 *
 *		The job must (also) match the socket specified and the host associated
 *		with the socket unless ji_fromsock == -1, then its a recovery situation.
 *
 * @param[in]	preq	-	The batch request structure
 * @param[in]	jobid	-	Job Id which needs to be located
 *
 * @return	job structure associated with jobid.
 */

void *
locate_new_job(struct batch_request *preq, char *jobid)
{
#ifndef PBS_MOM
	svrjob_t *pj;
#else
	job *pj;
#endif

	int sock = -1;
	pbs_net_t conn_addr = 0;

	if (preq == NULL)
		return NULL;

	sock = preq->rq_conn;

	if (!preq->prot) { /* Connection from TCP stream */
		conn_addr = get_connectaddr(sock);
	} else {
		struct sockaddr_in *addr = tpp_getaddr(sock);
		if (addr)
			conn_addr = (pbs_net_t) ntohl(addr->sin_addr.s_addr);
	}

	pj = GET_NEXT(svr_newjobs);
	while (pj) {
		if ((pj->ji_qs.ji_un.ji_newt.ji_fromsock == -1) ||
		    ((pj->ji_qs.ji_un.ji_newt.ji_fromsock == sock) &&
		     (pj->ji_qs.ji_un.ji_newt.ji_fromaddr == conn_addr))) {

			if (jobid != NULL) {
				if (!strncmp(pj->ji_qs.ji_jobid, jobid, PBS_MAXSVRJOBID))
					break;
			} else
				break;
		}

		pj = GET_NEXT(pj->ji_alljobs);
	}
	return pj;
}

/**
 * @brief
 * 		job_init_wattr - initialize job working attribute array
 *		set the types and the "unspecified value" flag
 *
 * @see
 * 		job_alloc
 *
 * @param[in]	pj - pointer to job structure
 *
 * @return	void
 */
void
job_init_wattr(void *pjob)
{
	int i;
#ifdef PBS_MOM
	job *pj = pjob;
#else
	svrjob_t *pj = pjob;
#endif

	for (i = 0; i < (int) JOB_ATR_LAST; i++) {
		clear_attr(&pj->ji_wattr[i], &job_attr_def[i]);
	}
}

/**
 *  @brief
 *		Output credential into job file.
 *
 * @param[in]		pjob - pointer to job struct
 * @param[in]		cred - JobCredential
 * @param[in]		len - size of credentials.
 *
 * @return	int
 * @retval	0	- success
 * @retval	-1	- fail
 */
int
write_cred(void *pj, char *cred, size_t len)
{
	extern char *path_jobs;
	char name_buf[MAXPATHLEN + 1];
	int cred_fd;
	int ret = -1;
#ifdef PBS_MOM
	job *pjob = pj;
#else
	svrjob_t *pjob = pj;
#endif

	strcpy(name_buf, path_jobs);
	if (*pjob->ji_qs.ji_fileprefix != '\0')
		strcat(name_buf, pjob->ji_qs.ji_fileprefix);
	else
		strcat(name_buf, pjob->ji_qs.ji_jobid);
	strcat(name_buf, JOB_CRED_SUFFIX);

	if ((cred_fd = open(name_buf, O_WRONLY | O_CREAT | O_EXCL, 0600)) == -1) {
		log_err(errno, __func__, name_buf);
		return -1;
	}

#ifdef WIN32
	secure_file(name_buf, "Administrators", READS_MASK | WRITES_MASK | STANDARD_RIGHTS_REQUIRED);
	setmode(cred_fd, O_BINARY);
#endif

	if (write(cred_fd, cred, len) != len) {
		log_err(errno, __func__, "write cred");
		goto done;
	}

#if !defined(PBS_MOM) && defined(WIN32)
	cache_usertoken_and_homedir(pjob->ji_wattr[JOB_ATR_euser].at_val.at_str,
				    NULL, 0, read_cred, (svrjob_t *) pjob, pbs_decrypt_pwd, 1);
#endif
	ret = 0;

done:
	close(cred_fd);
	return ret;
}

/**
 * @brief
 *		Check if this job has an associated credential file.  If it does,
 *		the credential file is opened and the credential is read into
 *		malloc'ed memory.
 *
 * @param[in]		pjob - job whose credentials needs to be read.
 * @param[out]		cred - JobCredential
 * @param[in]		len - size of credentials.
 *
 * @return	int
 * @retval	1	- no cred
 * @retval	0	- success
 * @retval	-1	- error
 */
int
read_cred(void *pj, char **cred, size_t *len)
{
	extern char *path_jobs;
	char name_buf[MAXPATHLEN + 1];
	char *hold = NULL;
	struct stat sbuf;
	int fd;
	int ret = -1;
#ifdef PBS_MOM
	job *pjob = pj;
#else
	svrjob_t *pjob = pj;
#endif

	strcpy(name_buf, path_jobs);
	if (*pjob->ji_qs.ji_fileprefix != '\0')
		strcat(name_buf, pjob->ji_qs.ji_fileprefix);
	else
		strcat(name_buf, pjob->ji_qs.ji_jobid);
	strcat(name_buf, JOB_CRED_SUFFIX);

	if ((fd = open(name_buf, O_RDONLY)) == -1) {
		if (errno == ENOENT)
			return 1;
		log_err(errno, __func__, "open");
		return ret;
	}

	if (fstat(fd, &sbuf) == -1) {
		log_err(errno, __func__, "fstat");
		goto done;
	}

	hold = malloc(sbuf.st_size);
	assert(hold != NULL);

#ifdef WIN32
	setmode(fd, O_BINARY);
#endif

	if (read(fd, hold, sbuf.st_size) != sbuf.st_size) {
		log_err(errno, __func__, "read");
		goto done;
	}
	*len = sbuf.st_size;
	*cred = hold;
	hold = NULL;
	ret = 0;

done:
	close(fd);
	if (hold != NULL)
		free(hold);
	return ret;
}

/**
 * @brief
 *		Return 1 if there is no credential, 0 if there is and -1 on error.
 *
 * @param[in]	remote	- server name
 * @param[in]	jobp	- job whose credentials needs to be read.
 * @param[in]	from	- can have the following values,
 * 							PBS_GC_BATREQ, PBS_GC_CPYFILE and PBS_GC_EXEC
 * @param[out]	data	- kerberos credential
 * @param[out]	dsize	- kerberos credential data length
 *
 * @return	int
 * @retval	1	- there is no credential
 * @retval	0	- there is credential
 * @retval	-1	- error
 */
int
get_credential(char *remote, void *jp, int from, char **data, size_t *dsize)
{
	int ret;
#ifdef PBS_MOM
	job *jobp = jp;
#else
	svrjob_t *jobp = jp;
#endif

	switch (jobp->ji_extended.ji_ext.ji_credtype) {

	default:

#ifndef PBS_MOM

		/*   ensure job's euser exists as this can be called */
		/*   from pbs_send_job who is moving a job from a routing */
		/*   queue which doesn't have euser set */
		if ((jobp->ji_wattr[JOB_ATR_euser].at_flags & ATR_VFLAG_SET) &&
		    jobp->ji_wattr[JOB_ATR_euser].at_val.at_str) {
			ret = user_read_password(jobp->ji_wattr[(int) JOB_ATR_euser].at_val.at_str, data, dsize);

			/* we have credential but type is NONE, force DES */
			if (ret == 0 &&
			    (jobp->ji_extended.ji_ext.ji_credtype ==
			     PBS_CREDTYPE_NONE))
				jobp->ji_extended.ji_ext.ji_credtype =
				    PBS_CREDTYPE_AES;
		} else
			ret = read_cred(jobp, data, dsize);
#else
		ret = read_cred(jobp, data, dsize);
#endif
		break;
	}
	return ret;
}

/**
 * @brief	 Generic routine to allocate job object for server and mom
 *
 * @param	void
 * @return void *
 * @retval pointer to the job
 */
void *
job_alloc_generic(void)
{
#ifdef PBS_MOM
	return job_alloc();
#else
	return svrjob_alloc();
#endif
}

/**
 * @brief	Generic wrapper for job_free mom and server
 *
 * @pram[out]	pj - pointer to the job to free
 * @return void
 */
void
job_free_generic(void *pj)
{
#ifdef PBS_MOM
	job_free(pj);
#else
	svrjob_free(pj);
#endif
}

/**
 * @brief	Generic wrapper for job_purge mom and server
 *
 * @pram[out]	pj - pointer to the job to purge
 * @return void
 */
void
job_purge_generic(void *pj)
{
#ifdef PBS_MOM
	job_purge(pj);
#else
	job_purge_svr(pj);
#endif
}

/**
 * @brief	 Generic routine to find job for server and mom
 *
 * @param[in]	jid - id of the job
 * @return void *
 * @retval pointer to the job
 */
void *
find_job_generic(char *jid)
{
#ifdef PBS_MOM
	return find_job(jid);
#else
	return find_svrjob(jid);
#endif
}

/**
 * @brief	Convenience function to delete job related files for a job being purged
 *
 * @param[in]	pjob - the job being purged
 * @param[in]	fsuffix - suffix of the file to delete
 *
 * @return	void
 */
void
del_job_related_file(void *pj, char *fsuffix)
{
	extern char *msg_err_purgejob;
	char namebuf[MAXPATHLEN + 1] = {'\0'};
#ifdef PBS_MOM
	job *pjob = pj;
#else
	svrjob_t *pjob = pj;
#endif

	strcpy(namebuf, path_jobs);
	if (*pjob->ji_qs.ji_fileprefix != '\0')
		strcat(namebuf, pjob->ji_qs.ji_fileprefix);
	else
		strcat(namebuf, pjob->ji_qs.ji_jobid);
	strcat(namebuf, fsuffix);
	if (unlink(namebuf) < 0) {
		if (errno != ENOENT) {
			log_joberr(errno, __func__, msg_err_purgejob,
				   pjob->ji_qs.ji_jobid);
		}
	}
}

/**
 * @brief
 *	This function updates/creates the resource list named
 *	'res_list_name' and indexed in pjob as
 *	'res_list_index', using resources assigned values specified
 *	in 'exec_vnode'. This also saves the previous values in
 *	pjob's 'backup_res_list_index' attribute if not already
 *	set.
 *
 * @param[in,out] pjob - job structure
 * @param[in]	  res_list_name - resource list name
 * @param[in]	  rel_list_index - attribute index in job structure
 * @param[in]	  exec_vnode - string containing  the various resource
 *			assignments
 * @param[in]	  op - kind of operation to be performed while setting
 *		     the resource value.
 * @param[in]	  always_set  - if set, even if there is no resulting
 *			resource list, try to have at least one entry
 *			(e.g., ncpus=0) to keep the list set.
 * @param[in]	  backup_res_list_index - index to  job's attribute
 *			resource list to hold original values.
 *
 * @return int
 * @retval 0  - success
 * @retval 1 - failure
 */

int
update_resources_list(void *pj, char *res_list_name,
		      int res_list_index, char *exec_vnode, enum batch_op op,
		      int always_set, int backup_res_list_index)
{
	char *chunk;
	int j;
	int rc;
	int nelem;
	char *noden;
	struct key_value_pair *pkvp;
	resource_def *prdef;
	resource *presc, *pr, *next;
	attribute tmpattr;
#ifdef PBS_MOM
	job *pjob = pj;
#else
	svrjob_t *pjob = pj;
#endif

	if (exec_vnode == NULL || pjob == NULL) {
		log_err(PBSE_INTERNAL, __func__, "bad input parameter");

		return (1);
	}

	/* Save current resource values in backup resource list */
	/* if backup resources list is not already set */
	if (pjob->ji_wattr[res_list_index].at_flags & ATR_VFLAG_SET) {

		if ((pjob->ji_wattr[backup_res_list_index].at_flags & ATR_VFLAG_SET) == 0) {
			job_attr_def[backup_res_list_index].at_free(&pjob->ji_wattr[backup_res_list_index]);
			job_attr_def[backup_res_list_index].at_set(&pjob->ji_wattr[backup_res_list_index], &pjob->ji_wattr[res_list_index], INCR);
		}

		pr = (resource *) GET_NEXT(pjob->ji_wattr[res_list_index].at_val.at_list);
		while (pr != NULL) {
			next = (resource *) GET_NEXT(pr->rs_link);
			if (pr->rs_defin->rs_flags & (ATR_DFLAG_RASSN | ATR_DFLAG_FNASSN | ATR_DFLAG_ANASSN)) {
				delete_link(&pr->rs_link);
				if (pr->rs_value.at_flags & ATR_VFLAG_INDIRECT)
					free_str(&pr->rs_value);
				else
					pr->rs_defin->rs_free(&pr->rs_value);
				(void) free(pr);
			}
			pr = next;
		}
		pjob->ji_modified = 1;
	}

	rc = 0;
	for (chunk = parse_plus_spec(exec_vnode, &rc); chunk && (rc == 0);
	     chunk = parse_plus_spec(NULL, &rc)) {

		if ((rc = parse_node_resc(chunk, &noden, &nelem, &pkvp)) != 0) {
			log_err(rc, __func__, "parse of exec_vnode failed");
			goto update_resources_list_error;
		}
		for (j = 0; j < nelem; j++) {
			prdef = find_resc_def(svr_resc_def,
					      pkvp[j].kv_keyw, svr_resc_size);
			if (prdef == NULL) {
				snprintf(log_buffer, sizeof(log_buffer),
					 "unknown resource %s in exec_vnode",
					 pkvp[j].kv_keyw);
				log_err(PBSE_INTERNAL, __func__, log_buffer);
				goto update_resources_list_error;
			}

			if (prdef->rs_flags & (ATR_DFLAG_RASSN | ATR_DFLAG_FNASSN | ATR_DFLAG_ANASSN)) {
				presc = add_resource_entry(
				    &pjob->ji_wattr[res_list_index],
				    prdef);
				if (presc == NULL) {
					snprintf(log_buffer,
						 sizeof(log_buffer),
						 "failed to add resource"
						 "  %s",
						 prdef->rs_name);
					log_err(PBSE_INTERNAL, __func__,
						log_buffer);
					goto update_resources_list_error;
				}
				if ((rc = prdef->rs_decode(&tmpattr,
							   res_list_name, prdef->rs_name,
							   pkvp[j].kv_val)) != 0) {
					snprintf(log_buffer,
						 sizeof(log_buffer),
						 "decode of %s failed",
						 prdef->rs_name);

					log_err(PBSE_INTERNAL, __func__,
						log_buffer);
					goto update_resources_list_error;
				}
				(void) prdef->rs_set(&presc->rs_value,
						     &tmpattr, op);
			}
		}
	}

	if (rc != 0) {
		log_err(PBSE_INTERNAL, __func__, "error parsing exec_vnode");
		goto update_resources_list_error;
	}

	if (always_set &&
	    ((pjob->ji_wattr[res_list_index].at_flags & ATR_VFLAG_SET) == 0)) {
		/* this means no resources got freed during suspend */
		/* let's put a dummy entry for ncpus=0 */
		prdef = find_resc_def(svr_resc_def, "ncpus",
				      svr_resc_size);
		if (prdef == NULL) {
			log_err(PBSE_INTERNAL, __func__,
				"no ncpus in svr_resc_def!");
			return (1);
		}
		presc = add_resource_entry(
		    &pjob->ji_wattr[res_list_index], prdef);
		if (presc == NULL) {
			log_err(PBSE_INTERNAL, __func__,
				"failed to add ncpus in resource list");
			return (1);
		}
		if ((rc = prdef->rs_decode(&tmpattr, res_list_name,
					   prdef->rs_name, "0")) != 0) {
			log_err(rc, __func__,
				"decode of ncpus=0 failed");
			return (1);
		}
		(void) prdef->rs_set(&presc->rs_value, &tmpattr, op);
	}

	return (0);

update_resources_list_error:
	job_attr_def[backup_res_list_index].at_free(
	    &pjob->ji_wattr[backup_res_list_index]);
	pjob->ji_wattr[backup_res_list_index].at_flags &= ~ATR_VFLAG_SET;
	job_attr_def[res_list_index].at_set(
	    &pjob->ji_wattr[res_list_index],
	    &pjob->ji_wattr[backup_res_list_index], INCR);
	pjob->ji_modified = 1;
	return (1);
}
