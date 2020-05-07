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

#ifndef _SVRJOB_H
#define _SVRJOB_H
#ifdef __cplusplus
extern "C"
{
#endif

#include "attribute.h"
#include "job.h"

void set_attr_rsc_used_acct(svrjob_t*pjob);
void set_acct_resc_used(svrjob_t*pjob);
int create_acct_resc_used(const svrjob_t*pjob, char **resc_used, int *resc_used_size);

struct depend_job *find_dependjob(struct depend *pdep, char *name);
struct depend *find_depend(int type, attribute *pattr);
void free_depend(attribute *attr);

int depend_on_exec(svrjob_t*pjob);
int depend_on_term(svrjob_t*pjob);
int depend_on_que(attribute *pattr, void *pobj, int mode);

int depend_runone_remove_dependency(svrjob_t*pjob);
int depend_runone_hold_all(svrjob_t*pjob);
int depend_runone_release_all(svrjob_t*pjob);

void post_runone(struct work_task *pwt);
void req_register_dep(struct batch_request *);
int send_depend_req(svrjob_t*pjob, struct depend_job *pparent, int type, int op, int schedhint, void (*postfunc)(struct work_task *));

#ifdef __cplusplus
}
#endif
#endif /* _SVRJOB_H */
