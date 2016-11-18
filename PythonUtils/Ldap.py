

__all__ = ('LdapWrapper',)

import ldap
import ldap.modlist
import json


class LdapWrapper(object):
    def __init__(self, host, ldaps=False):
        port = 389
        if ldaps:
            ldap.set_option(ldap.OPT_X_TLS_REQUIRE_CERT, ldap.OPT_X_TLS_NEVER)
            port = 636

        url = '%s://%s:%d' % (('ldaps' if ldaps else 'ldap'), host, port)

        self.__ldapobj = ldap.initialize(url)
        self.__ldapobj.protocol_version = ldap.VERSION3

        self.__bund = False


    def bind(self, bind_dn, bind_pw):
        self.__ldapobj.simple_bind_s(bind_dn, bind_pw)
        self.__bund = True


    def unbind(self):
        if self.__bund:
            self.__ldapobj.unbind_s()
            self.__bund = False


    def search(self, base_dn, search_scope=ldap.SCOPE_SUBTREE,
               search_filter='(objectClass=*)', attrs=None, limit=0):
        '''  do a ldapsearch, return all the matched entries in a list'''
        rid = self.__ldapobj.search(base_dn, search_scope, search_filter, attrs)

        count = 0
        while True:
            count += 1
            if limit > 0 and count > limit:
                return

            r_type, r_data = self.__ldapobj.result(rid, 0)
            if r_type == ldap.RES_SEARCH_ENTRY:
                yield r_data[0]
            else:
                return


    def add(self, add_dn, objectClass, **attrs):
        if not self.__bund:
            raise RuntimeError('Ldap is unbund, cannot add')

        attrs['objectClass'] = objectClass
        ldif = ldap.modlist.addModlist(attrs)
        try:
            self.__ldapobj.add_s(add_dn, ldif)
        except Exception as e:
            if __debug__: print e
            pass


    def delete(self, bind_dn, bind_pw, del_dn):
        if not self.__bund:
            raise RuntimeError('Ldap is unbund, cannot add')

        try:
            self.__ldapobj.delete_s(del_dn)
        except Exception as e:
            if __debug__: print e
            pass


    def modify(self, mod_dn, attr, oldv, newv):
        if not self.__bund:
            raise RuntimeError('Ldap is unbund, cannot add')

        ldif = ldap.modlist.modifyModlist({attr : oldv}, {attr : newv})
        try:
            self.__ldapobj.modify_s(mod_dn, ldif)
        except Exception as e:
            if __debug__: print e
            pass


    def rename(self, dn, newdn):
        if not self.__bund:
            raise RuntimeError('Ldap is unbund, cannot add')

        try:
            self.__ldapobj.renames(dn, newdn)
        except Exception as e:
            if __debug__: print e
            pass


    def valid_user(self, base_dn, uid, user_pass, uid_field='uid'):
        try:
            search_filter='(%s=%s)' % (uid_field, uid)
            print search_filter
            entries = self.__ldapobj.search_s(base_dn, ldap.SCOPE_SUBTREE,
                                            search_filter, [])

            if len(entries) == 0:
                print 'No such user'
                return False


            user_dn = entries[0][0]
            bund = self.__bund

            self.bind(user_dn, user_pass)

            if __debug__:
                print json.dumps(entries[0], indent=4)

            if not bund:
                self.unbind()

            return True
        except Exception as e:
            if __debug__: print e
            return False


#
# ------ main ------
if __name__ == '__main__':
    import sys
    from optparse import OptionParser

    parser = OptionParser()
    parser.add_option('--host', default='ldap.hp.com') # HPE
    parser.add_option('--ldaps', action='store_true', default=False)
    parser.add_option('-b', '--basedn', default='o=hp.com')
    parser.add_option('--limit', type=int, default=100)
    parser.add_option('--user')
    parser.add_option('--password')

    opts, args = parser.parse_args()
    if opts.basedn is None:
        print 'basedn cannot be empty'
        sys.exit(1)

    lw = LdapWrapper(opts.host, opts.ldaps)

    #valid user
    if opts.user and opts.password:
        print lw.valid_user(opts.basedn, opts.user, opts.password,
                           'hpUnixUserName')
        sys.exit(0)

#   #print all groups
#   print 'Groups: (first 100)'
#   base_dn = 'ou=Groups,' + opts.basedn
#   for dn, entry in lw.search(base_dn, search_scope=ldap.SCOPE_ONELEVEL,
#                              search_filter='(cn=*)',
#                              limit=100):
#       group = {'dn' : dn,
#                'cn' : entry['cn'][0]}

#       print group

    #print all users
    print 'Users: (first %d)' % opts.limit
    base_dn = 'ou=People,' + opts.basedn
    for dn, entry in lw.search(base_dn, search_scope=ldap.SCOPE_ONELEVEL,
                               search_filter='(hpUnixUserName=*)',
                               limit=opts.limit):
        user = {'dn': dn, 'uid' : entry['uid'][0]}

        if entry.get('hpUnixUserName', None):
            user['user'] = entry['hpUnixUserName'][0]

        print user



