"""this will ping thr ldap server to obtain openx userrs"""


import subprocess
import pandas as pd
import numpy as np
import networkx as nx


class EmployeeAccess(object):
    """openx employee access"""

    def __init__(self):
        """init"""
        # grab all ldap users and human employees
        self.ldapUsers = self.__getLdapUsers()
        self.employees = self.__getEmployees()
        self.employeeGraph = self.__getEmployeeGraph()

    @classmethod
    def __pingLdapServer(cls):
        """
        """
        cmd = ['ldapsearch', '-b', 'ou=Users,dc=openx,dc=org', '-h',
               'directory.prod.gcp.openx.org', '-x', '-p', '389']
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE)
        out = p.communicate()[0].decode()

        return out

    def __getLdapUsers(self):
        """
        """
        # collect a list of active openx employee emails, taking into account
        # alternate names to sync with snowflake
        out = self.__pingLdapServer()
        users = [item for item in out.split('\n\n')[3:-2]]

        data = []
        for user in users:
            cn = np.nan
            email = np.nan
            hireDate = np.nan
            headLess = False
            displayName = np.nan
            alternateEmail = np.nan
            manager = np.nan
            for row in user.split('\n'):
                if row.startswith('cn: '):
                    cn = row.replace('cn: ', '')
                if row.startswith('mail: '):
                    email = row.replace('mail: ', '')
                    if email == 'null' and cn:
                        email = '%s@openx.com' % cn
                if row.startswith('openxHireDate: '):
                    hireDate = pd.to_datetime(row.replace('openxHireDate: ', '')).to_pydatetime()
                if row.startswith('objectClass: openxHeadless'):
                    headLess = True
                if row.startswith('displayName: '):
                    displayName = row.replace('displayName: ', '')
                if row.startswith('manager: '):
                    manager = row.split('manager: cn=')[1].split(',')[0]

            if pd.notnull(displayName):
                alternateEmail = '%s@openx.com' % displayName.replace('-', '').replace(' ', '.')
                if alternateEmail == email:
                    alternateEmail = np.nan
            data.append([cn, hireDate, headLess, email, displayName, alternateEmail, manager])
        cols = ['company_name', 'hire_date', 'is_headless', 'email', 'display_name',
                'alternate_email', 'manager']
        df = pd.DataFrame(data, columns=cols)

        return df

    def __getEmployees(self):
        """
        """
        employees = self.ldapUsers[(self.ldapUsers['hire_date'].notnull()) &
                                   (~self.ldapUsers['is_headless']) &
                                   (self.ldapUsers['email'].notnull())]
        employees = employees[employees['email'].str.contains('@openx.com')]
        employees = employees[(~employees['company_name'].str.contains('test')) &
                              (~employees['company_name'].str.contains('iridium'))]
        return employees.reset_index(drop=True)

    def __getEmployeeGraph(self):
        """
        """
        G = nx.DiGraph()
        for i in self.employees.index:
            cn = self.employees.loc[i, 'company_name']
            mgr = self.employees.loc[i, 'manager']
            em = self.employees.loc[i, 'email']
            altEm = self.employees.loc[i, 'alternate_email']
            G.add_node(cn, email=em, alt_email=altEm)
            if pd.notnull(mgr):
                G.add_node(mgr)
                G.add_edge(mgr, cn)

        return G