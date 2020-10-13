import cherrypy
import os,sys
import config, queryLDAP
from cherrypy.lib import auth_digest
from SlurmMinitorUI   import SLURMMinitorUI
from SlurmMinitorData import SLURMMinitorData

class HelloWorld(object):
    @cherrypy.expose
    def index(self):
        if 'count' in cherrypy.session:
           cherrypy.session['count'] += 1  
        else:
           cherrypy.session['count'] = 1
        return "Hello world!"

    @cherrypy.expose
    def test(self):
        if 'count' in cherrypy.session:
           cherrypy.session['count'] += 1  
        else:
           cherrypy.session['count'] = 1
        return '{} test {}'.format(cherrypy.session['user'], cherrypy.session['count'])

    @cherrypy.expose
    def test1(self):
        if 'count' in cherrypy.session:
           cherrypy.session['count'] += 1  
        else:
           cherrypy.session['count'] = 1
        return '{} test1 {}'.format(cherrypy.session['user'], cherrypy.session['count'])

class HelloWorld1(object):
    @cherrypy.expose
    def index(self):
        if 'count' in cherrypy.session:
           cherrypy.session['count'] += 1  
        else:
           cherrypy.session['count'] = 1
        return "Hello world1! {}".format(cherrypy.session.get('user','No user'))

def validate_password(realm, username, passwd):
    print("validate_password {}".format(username))
    cherrypy.session['user']=username
    if queryLDAP.ldap_validate(username, passwd):
       # if user's setting file exist, get it, otherwise set to default one
       cherrypy.session['settings'] = config.getUserSettings(username)
       print("{} settings {}".format(username, cherrypy.session['settings']))
       return True
    else:
       return False

def error_page_500(status, message, traceback, version):
    return "Error %s - Well, I'm very sorry but the page your requested is not implemented!" % status

#global config
cherrypy.config.update({#'environment': 'production',
                        'log.access_file':                '/tmp/slurm_util/testLDAP.log',
                        'log.screen':                     True,
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
   '/': {
       'tools.auth_basic.on': True,
       'tools.auth_basic.realm': 'localhost',
       'tools.auth_basic.checkpassword': validate_password,
       'tools.auth_basic.accept_charset': 'UTF-8',
    },
    '/favicon.ico': {
        'tools.staticfile.on': True,
        'tools.staticfile.filename': os.path.join(config.APP_DIR, 'public/images/sf.ico'),
    },
}
conf1 = {
    '/static': {
        'tools.staticdir.on': True,
        'tools.staticdir.dir': os.path.join(config.APP_DIR, 'public'),
    },
    '/favicon.ico': {
        'tools.staticfile.on': True,
        'tools.staticfile.filename': os.path.join(config.APP_DIR, 'public/images/sf.ico'),
    },
}

if __name__ == '__main__':
    cherrypy.tree.mount(HelloWorld(),   '/', conf)
    cherrypy.tree.mount(HelloWorld1(), '/1', conf1)

    cherrypy.engine.start()
    cherrypy.engine.block()
    #cherrypy.quickstart(HelloWorld(), '/', conf)
    #cherrypy.quickstart(WebMonitor.SLURMMonitor(), '/', conf)
