import sys

import ldap
import ldap.filter

def ldap_validate(username, password, server="ldap://ldap1.flatironinstitute.org"):
    #print("{}:{}".format(username, password))
    try:
        ldapconn                  = ldap.initialize(server)
        ldapconn.protocol_version = ldap.VERSION3
    except ldap.LDAPError as e:
        print(e) # couldn't connect to ldap server, use app.logger in production
        return False
    username = ldap.filter.escape_filter_chars(username, escape_mode=1)
    password = ldap.filter.escape_filter_chars(password)
    dn = "uid={},ou=People,dc=flatironinstitute,dc=org".format(username)
    try:
        ldapconn.simple_bind_s(dn, password) # we are using starttls right?
        return True
    except ldap.LDAPError as e:
        print(e) # wrong password, use app.logger in production
        return False
    return False

if __name__ == '__main__':
    ldap_validate(sys.argv[1], sys.argv[2])

