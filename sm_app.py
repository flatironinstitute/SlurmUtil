import cherrypy
import os,sys
import config, queryLDAP
from cherrypy.lib import auth_digest
from SlurmMonitorUI   import SLURMMonitorUI
from SlurmMonitorData import SLURMMonitorData

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

class StringGenerator(object):
    @cherrypy.expose
    def index(self):
        return """<html>
          <head>
            <link href="/static/css/style.css" rel="stylesheet">
          </head>
            <body>
                <div class="layout">
                <nav class="nav">
                    <ul class="nav__inner">
                    <li class="nav__item nav__item--active"><a class="nav__link">Summary</a></li>
                    <li class="nav__item"><a class="nav__link">Host Util</a></li>
                    <li class="nav__item"><a class="nav__link">Pending Jobs</a></li>
                    <li class="nav__item"><a class="nav__link">Sunburst Graph</a></li>
                    <li class="nav__item"><a class="nav__link">File Usage</a></li>
                    <li class="nav__item"><a class="nav__link">Tymor</a></li>
                    <li class="nav__item"><a class="nav__link">Bulletin Board</a></li>
                    <li class="nav__item"><a class="nav__link">Report</a></li>
                    <li class="nav__item"><a class="nav__link">Forecast</a></li>
                    <li class="nav__item"><a class="nav__link">Settings</a></li>
                    <li class="nav__item nav__item--search"><a class="nav__link nav__link--search"></a></li>
                    </ul>
                </nav>
                <div class="crumb__container">
                    <div class="crumb__inner">
                    <div class="crumb__content">
                        <div class="crumbs">
                        <a href="/" class="crumb">Home</a>
                        <a href="/" class="crumb">Things</a>
                        <a href="/" class="crumb">Frings</a>
                        </div>
                    </div>
                    </div>
                </div>
                <div class="hero">
                    <p class="hero__text hero__text--top"><span class="hl">Flatiron Institute</span> computing cluster</p>
                    <h1 class="magic">
                    <a href="/" class="hero__title">Slurm Util</a>
                    </h1>
                    <p class="hero__text">
                    Explore <a class="hl" href="/">allocations</a> and <a class="hl" href="/">activity</a> on
                    <a class="hl" href="https://slurm.schedmd.com/documentation.html">Slurm Workload Manager</a>
                    </p>
                    <p class="hero__text hero__text--small">Watch what happens live</p>
                </div>
                <section class="section">
                    <div class="section__inner">
                    <div class="dashboard">
                        <div class="dashboard__section">
                        <h3>Dashboard here</h3>
                        <p>Carrot cake pie bear claw cake halvah muffin carrot cake liquorice toffee. Tootsie roll jelly-o sweet dragée muffin gingerbread croissant ice cream brownie. Danish sweet roll donut danish. Donut cheesecake donut liquorice gummi bears donut.</p>
                        </div>
                        <div class="dashboard__section">
                        <h3>Dashboard there</h3>
                        <p>Sweet roll fruitcake tootsie roll sweet danish oat cake. Liquorice cake gingerbread ice cream jelly beans lemon drops marshmallow chocolate bar dragée. Cotton candy jelly beans sweet.</p>
                        </div>
                        <div class="dashboard__section">
                        <h3>Dashboard everywhere</h3>
                        <p>Marshmallow caramels cookie halvah fruitcake pastry brownie jelly-o. Icing powder gummi bears toffee. Tiramisu danish bonbon topping pastry sugar plum. Gummi bears toffee macaroon icing sugar plum lemon drops.
                        </p>
                        </div>
                    </div>
                    </div>
                </section>
                <section class="section">
                    <div class="section__inner">
                    <div class="grid">
                        <div class="grid__title">
                        <h2 class="title title--grid">Frequently, Content is Fake.</h2>
                        </div>
                        <div class="grid__item">
                        <h3>What is Lorem Ipsum?</h3>
                        <p>
                            Lorem Ipsum is simply dummy text of the printing and typesetting industry. Lorem Ipsum has been the industry's standard dummy text ever since the 1500s, when an unknown printer took a galley of type and scrambled it to make a type specimen book.
                        </p>
                        </div>
                        <div class="grid__item">
                        <h3>Where does it come from?</h3>
                        <p>Contrary to <a>popular belief</a>, Lorem Ipsum is not simply random text. It has roots in a piece of classical Latin literature from 45 BC, making it over 2000 years old.
                        </p>
                        </div>
                        <div class="grid__item">
                        <h3>Why do we use it?</h3>
                        <p>
                            It is a long established fact that a reader will be distracted by the readable content of a page when looking at its layout. The point of using Lorem Ipsum is that it has a more-or-less normal distribution of letters, as opposed to using 'Content here, content here', making it look like readable English.
                        </p>
                        </div>
                        <div class="grid__item">
                        <h3>Where can I get some?</h3>
                        <p>
                            There are many variations of passages of Lorem Ipsum available, but the majority have suffered alteration in some form, by injected humour, or randomised words which don't look even slightly believable. If you are going to use a passage of Lorem Ipsum, you need to be sure there isn't anything embarrassing hidden in the middle of text.
                        </p>
                        </div>
                        <div class="grid__item">
                        <h3>I have some other question! What do I do?</h3>
                        <p>
                            Email us at <a href="mailto:scicomp@flatironinstitute.org">scicomp</a> or join the <b>scicomp</b> channel on the Simons Foundation Slack.
                        </p>
                        </div>
                    </div>
                    </div>
                </section>
                <section class="section section--footer">
                    <div class="section__inner section__inner--footer">
                    <a href="/" class="hero__title hero__title--footer">Slurm Util</a>
                    <p class="hero__text hero__text--small" id="signature"></p>
                    </div>
                </footer>
                </div>
            </body>
              <script>
                const now = new Date();
                const year = now.getFullYear();
                const signature = `Flatiron Institute, ` + year;
                const sig = document.querySelector(`#signature`);
                sig.innerHTML = signature;
            </script>
        </html>"""

if __name__ == '__main__':
   #global config
   cherrypy.config.update({#'environment': 'production',
                        'log.access_file':                '',
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
   sm_data = SLURMMonitorData()
   cherrypy.tree.mount(SLURMMonitorUI(sm_data), '/',     conf)
   cherrypy.tree.mount(sm_data,                 '/data', conf1)

   #cherrypy.engine.signals.subscribe()
   cherrypy.engine.start()
   cherrypy.engine.block()
