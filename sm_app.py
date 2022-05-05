import cherrypy
import os,sys
import config, queryLDAP
from cherrypy.lib import auth_digest
from SlurmMonitorUI   import SLURMMonitorUI
from SlurmMonitorData import SLURMMonitorData

#not used 
def validate_password(realm, username, passwd):
    cherrypy.session['user']=username
    if queryLDAP.ldap_validate(username, passwd):
       # if user's setting file exist, get it, otherwise set to default one
       cherrypy.session['settings'] = config.getUserSettings(username)
       return True
    else:
       return False

def error_page_500(status, message, traceback, version):
    return "Error %s - Well, I'm very sorry but the page your requested is not implemented!" % status

if __name__ == '__main__':
   #global config
   cherrypy.config.update({#'environment': 'production',
                        'log.access_file':                './sm_app_access.log',
                        'log.screen':                     False,
#                        'error_page.500':                error_page_500,
                        'tools.sessions.on':              True,
#                        'tools.sessions.storage_type':    "File",
#                        'tools.sessions.storage_path':    os.path.join(config.APP_DIR, 'sessions'),
#                        'tools.sessions.timeout':         1440,
                        'server.socket_host':             '0.0.0.0',
                        'server.socket_port':             config.APP_CONFIG['port']})
   conf = {
    '/static': {
        'tools.staticdir.on': True,
        'tools.staticdir.dir': os.path.join(config.APP_DIR, 'public'),
    },
#   '/': {
#       'tools.auth_basic.on': True,
#       'tools.auth_basic.realm': 'localhost',
#       'tools.auth_basic.checkpassword': validate_password,
#       'tools.auth_basic.accept_charset': 'UTF-8',
#    },
    '/favicon.ico': {
        'tools.staticfile.on': True,
        'tools.staticfile.filename': os.path.join(config.APP_DIR, 'public/images/sf.ico'),
    },
   }
   conf1 = {
    '/static': {
        'tools.staticdir.on': True,
        'tools.staticdir.dir': os.path.join(config.APP_DIR, 'public'),
    }
   }

   print("config={}".format(config.APP_CONFIG))
   sm_flatiron  = SLURMMonitorData('Rusty')
   print("create /data")
   sm_popeye    = SLURMMonitorData('Popeye')
   print("create /popeye")
   cherrypy.tree.mount(SLURMMonitorUI(sm_flatiron, sm_popeye), '/',     conf)
   cherrypy.tree.mount(sm_flatiron,             '/data', conf1)
   cherrypy.tree.mount(sm_popeye,               '/popeye', conf1)

   #cherrypy.engine.signals.subscribe()
   cherrypy.engine.start()
   cherrypy.engine.block()
