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
       return config.APP_CONFIG["display_default"][attr]

def getSettings():
    if 'user' in cherrypy.session:
       return cherrypy.session["settings"];
    else:
       return config.APP_CONFIG["display_default"]

def setSetting (key,settings):
    if 'user' in cherrypy.session:
       cherrypy.session['settings'][key] = settings
       savUserSettings (cherrypy.session['user'],cherrypy.session['settings'])
    else:
       config.APP_CONFIG["display_default"][key]= settings


