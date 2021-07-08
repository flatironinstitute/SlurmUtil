import cherrypy
import config

def getUser ():
    if 'user' in cherrypy.session:
       return cherrypy.session["user"];
    else:
       return None

def getSetting(attr):
    if 'user' in cherrypy.session:
       return cherrypy.session["settings"][attr];
    else:
       return config.APP_CONFIG["settings"][attr]

def getSettings():
    if 'user' in cherrypy.session:
       return cherrypy.session["settings"];
    else:
       return config.APP_CONFIG["settings"]

def setSetting (key,settings):
    if 'user' in cherrypy.session:
       cherrypy.session['settings'][key] = settings
       savUserSettings (cherrypy.session['user'],cherrypy.session['settings'])
    else:
       APP_CONFIG["settings"][key]= settings


