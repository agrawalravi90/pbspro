# coding: utf-8

# Copyright (C) 1994-2020 Altair Engineering, Inc.
# For more information, contact Altair at www.altair.com.
#
# This file is part of both the OpenPBS software ("OpenPBS")
# and the PBS Professional ("PBS Pro") software.
#
# Open Source License Information:
#
# OpenPBS is free software. You can redistribute it and/or modify it under
# the terms of the GNU Affero General Public License as published by the
# Free Software Foundation, either version 3 of the License, or (at your
# option) any later version.
#
# OpenPBS is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Affero General Public
# License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
# Commercial License Information:
#
# PBS Pro is commercially licensed software that shares a common core with
# the OpenPBS software.  For a copy of the commercial license terms and
# conditions, go to: (http://www.pbspro.com/agreement.html) or contact the
# Altair Legal Department.
#
# Altair's dual-license business model allows companies, individuals, and
# organizations to create proprietary derivative works of OpenPBS and
# distribute them - whether embedded or bundled with other software -
# under a commercial license agreement.
#
# Use of Altair's trademarks, including but not limited to "PBS™",
# "OpenPBS®", "PBS Professional®", and "PBS Pro™" and Altair's logos is
# subject to Altair's trademark licensing policies.


from tests.functional import *


@requirements(num_moms=2)
class TestJobRequeueTimeoutErrorMsg(TestFunctional):
    """
    This test suite is for testing the new job_requeue_timeout error
    message that informs the user that the job rerun process is still
    in progress.
    """

    def setUp(self):
        TestFunctional.setUp(self)

        if len(self.moms) != 2:
            self.skip_test(reason="need 2 mom hosts: -p moms=<m1>:<m2>")

        self.server.set_op_mode(PTL_CLI)

        # PBSTestSuite returns the moms passed in as parameters as dictionary
        # of hostname and MoM object
        self.momA = self.moms.values()[0]
        self.momB = self.moms.values()[1]
        self.momA.delete_vnode_defs()
        self.momB.delete_vnode_defs()

        self.hostA = self.momA.shortname
        self.hostB = self.momB.shortname

        self.server.manager(MGR_CMD_DELETE, NODE, None, "")

        islocal = self.du.is_localhost(self.hostA)
        if islocal is False:
            self.server.manager(MGR_CMD_CREATE, NODE, id=self.hostA)
        else:
            self.server.manager(MGR_CMD_CREATE, NODE, id=self.hostB)

        self.server.manager(MGR_CMD_SET, SERVER, {'job_requeue_timeout': 1})

    def test_error_message(self):
        j = Job(TEST_USER, attrs={ATTR_N: 'job_requeue_timeout'})

        test = []
        test += ['dd if=/dev/zero of=file bs=1024 count=0 seek=10240\n']
        test += ['cat file\n']
        test += ['sleep 30\n']

        j.create_script(test, hostname=self.server.client)
        jid = self.server.submit(j)

        self.server.expect(
            JOB, {'job_state': 'R', 'substate': 42}, id=jid, max_attempts=30,
            interval=2)
        try:
            self.server.rerunjob(jid)
        except PbsRerunError as e:
            self.assertTrue('qrerun: Response timed out. Job rerun request ' +
                            'still in progress for' in e.msg[0])
