# Django settings for kantele project.
import os
from urllib.parse import urlsplit

TESTING = int(os.environ.get('TESTING', 0))
# local box setup
APIKEY = os.environ.get('APIKEY')

# File storage of raw files/analysis results
STORAGESHARES = os.environ.get('STORAGESHARES', '').split(',')
ANALYSISSHARE = os.environ.get('ANALYSISSHARE')
MZML_INSHARE = os.environ.get('MZMLINSHARE')
TMPSHARE = os.environ.get('TMPSHARE', False)
ARCHIVESHARE = os.environ.get('ARCHIVESHARE', False)
BACKUPSHARE = os.path.join(TMPSHARE, 'pdc_archive_links') if TMPSHARE else False

# File storage only used in web and nginx containers
# which is where uploaded result files live for web access (HTML reports)
WEBSHARE = os.environ.get('SERVABLE_FILE_PATH')

# Tmp file storage for uploaded files to be transported to storage
TMP_UPLOADPATH = os.environ.get('TMP_UPLOADPATH')
HOST_UPLOADDIR = os.environ.get('HOST_UPLOADDIR')

# DSM backups to tape
DSM_DIR = os.environ.get('DSM_DIR')

# site infra
STORAGECLIENT_APIKEY = os.environ.get('STORAGECLIENT_APIKEY')
ANALYSISCLIENT_APIKEY = os.environ.get('ANALYSISCLIENT_APIKEY')
ADMIN_APIKEY = os.environ.get('ADMIN_APIKEY')
CLIENT_APIKEYS = [STORAGECLIENT_APIKEY, ANALYSISCLIENT_APIKEY, ADMIN_APIKEY]
QUEUE_STORAGE = os.environ.get('QUEUE_STORAGE', 'mv_md5_storage')
QUEUE_FILE_DOWNLOAD = os.environ.get('QUEUE_FILE_DOWNLOAD', 'file_download')
QUEUE_BACKUP = os.environ.get('QUEUE_BACKUP', 'backup_archive')
QUEUE_NXF = os.environ.get('QUEUE_NXF', 'nextflow')
QUEUE_QC_NXF = os.environ.get('QUEUE_QC_NXF', 'qc_nextflow')
QUEUE_SEARCH_INBOX = os.environ.get('QUEUE_SEARCH_INBOX', 'scaninbox')

PROTOCOL = os.environ.get('PROTOCOL', 'https')
KANTELEHOST_NOPROTO = os.environ.get('KANTELEHOST', False)
KANTELEHOST = f'{PROTOCOL}://{KANTELEHOST_NOPROTO}'
RSYNC_HOST = os.environ.get('RSYNC_HOST', urlsplit(KANTELEHOST).netloc)
RSYNC_SSHUSER = os.environ.get('RSYNC_SSHUSER')
RSYNC_SSHKEY = os.environ.get('RSYNC_SSHKEY')
RSYNC_SSHPORT = os.environ.get('RSYNC_SSHPORT')

SECONDARY_STORAGE_RSYNC_USER = os.environ.get('SECONDARY_STORAGE_RSYNC_USER')
SECONDARY_STORAGE_RSYNC_KEY = os.environ.get('SECONDARY_STORAGE_RSYNC_KEY')

UPLOAD_URL = 'uploads'
TMPSHARENAME = 'tmp'
ARCHIVESHARENAME = 'archive'
ANALYSISSHARENAME = 'analysis'
MZMLINSHARENAME = 'mzml_in'
WEBSHARENAME = 'web'
# TODO maybe have datatype/storage DB setup
STORAGESHARENAMES = os.environ.get('STORAGESHARENAMES', '').split(',')
PRIMARY_STORAGESHARENAME = os.environ.get('PRIMARY_STORAGE')

SHAREMAP = {TMPSHARENAME: TMPSHARE,
            ARCHIVESHARENAME: ARCHIVESHARE,
            ANALYSISSHARENAME: ANALYSISSHARE,
            MZMLINSHARENAME: MZML_INSHARE,
            WEBSHARENAME: WEBSHARE,
            **{name: share for name, share in zip(STORAGESHARENAMES, STORAGESHARES)}
            }
TMPPATH = ''
ARCHIVEPATH = ''

NGINX_ANALYSIS_REDIRECT = os.environ.get('NGINX_ANALYSIS_REDIRECT')
SERVABLE_FILENAMES = ['qc.html', 'qc_full.html', 'qc_light.html', 'pipeline_report.html', 'report.html']

SLACK_BASE = 'https://hooks.slack.com/services/'
SLACK_WORKSPACE = os.environ.get('SLACK_WORKSPACE')
SLACK_HOOKS = {k.replace('SLACK_HOOK_', ''): v for k,v in os.environ.items() if k.startswith('SLACK_HOOK_')}
# message queue
RABBIT_HOST = os.environ.get('RABBITHOST')
RABBIT_VHOST = os.environ.get('RABBIT_VHOST')
RABBIT_USER = os.environ.get('RABBITUSER')
RABBIT_PASSWORD = os.environ.get('RABBITPASS')
CELERY_BROKER_URL = 'amqp://{}:{}@{}:5672/{}'.format(RABBIT_USER, RABBIT_PASSWORD, RABBIT_HOST, RABBIT_VHOST)
CELERY_TASK_SERIALIZER = 'json'
CELERY_RESULT_BACKEND = 'rpc'

# default prefetch is 4, but long running tasks e.g. analyses can disable and set to 1
CELERY_WORKER_PREFETCH_MULTIPLIER = os.environ.get('CELERY_PREFETCH', 4)

JOBRUNNER_INTERVAL = 5

# Lifespan for mzMLs and Instrument-QC RAW files, in days
MAX_MZML_STORAGE_TIME_POST_ANALYSIS = int(os.environ.get('MAX_MZML_STORAGE_TIME_POST_ANALYSIS', -1))
MAX_MZML_LC_STORAGE_TIME = int(os.environ.get('MAX_MZML_LC_STORAGE_TIME', -1))
MAX_MZML_QC_STORAGE_TIME = int(os.environ.get('MAX_MZML_QC_STORAGE_TIME', -1))

# Upload token lifespan
MAX_TIME_UPLOADTOKEN = 24 * 3600 # for user uploads
MAX_TIME_PROD_TOKEN = 30 * 24 * 3600 # for internal production instruments
TOKEN_RENEWAL_WINDOW_DAYS = 7

# Labelcheck experiment name
LCEXPNAME = '__labelchecks'
LC_DTYPE_IDS = [int(x) for x in os.environ.get('LC_DTYPE_ID', '-1').split(',')]

# QC files storage
QC_STORAGE_DIR = 'Instrument QC'

# local datasets 
LOCAL_PTYPE_ID = int(os.environ.get('LOCAL_PTYPE_ID', -1))

# Allowed characters for runs, project, experiment names
# formatted for use in regexp 
ALLOWED_PROJEXPRUN_CHARS = 'A-Za-z0-9-_'

# external datasets
ENSEMBL_API = 'https://rest.ensembl.org/info/software'
# human, canonical/isoform, only swiss
UNIPROT_API = 'https://rest.uniprot.org/uniprotkb/stream?download=true&format=fasta&query=(proteome:{}){}'
UP_ORGS = {'Homo sapiens': 'UP000005640', 'Mus musculus': 'UP000000589'}
ENSEMBL_DL_URL = 'ftp://ftp.ensembl.org/pub/release-{}/fasta/{}/pep/'
BIOMART_URL = 'https://ensembl.org/biomart/martservice'
PX_PROJECT_ID = os.environ.get('PX_PROJECT_ID')
# multiple
EXTERNAL_PRODUCER_IDS = [int(x) for x in os.environ.get('EXTERNAL_PRODUCER_IDS', '-1').split(',')]
USERFILEDIR = 'uploadfiles'

# nextflow
NXF_COMMAND = os.environ.get('NXF_COMMAND', 'nextflow')
LIBRARY_FILE_PATH = 'databases'
ANALYSIS_STAGESHARE = os.environ.get('STAGESHARE', False)
NF_RUNDIR = os.environ.get('NEXTFLOW_RUNDIR')

# hardcoded name for filetypes fasta DBs, analysis output
DBFA_FT_NAME = 'database'
ANALYSIS_FT_NAME = 'analysis_output'
THERMORAW = os.environ.get('THERMORAW', 'thermo_raw_file')
BRUKERRAW = os.environ.get('BRUKERRAW', 'bruker_raw_file')
## The following should probably be coded in MSInstrument and settable in admin
BRUKERKEY = os.environ.get('BRUKERKEY') # e.g. Description, SampleName
THERMOKEY = os.environ.get('THERMOKEY') # e.g. SampleID
PROCNAMES = {THERMORAW: ['ThermoFisher.Foundation.AcquisitionService.exe'],
        BRUKERRAW: ['timsEngine.exe'],
        }
# Injection waiting time until file becomes "findable" by psutil Proc.open_files(), in seconds
# Bruker just gets an extra minute to rule out problem
INJ_WAITTIMES = {THERMORAW: os.environ.get('THERMO_WAIT', 15 * 60), BRUKERRAW: os.environ.get('BRUKERWAIT', 60)}

# The thermo reader command
THERMOREADER_DOCKER = os.environ.get('THERMOREAD_DOCKER', 'ghcr.io/lehtiolab/kantele-thermorawreader')
THERMO_CLASSIFY_CMD = ['wine', 'ScanHeadsman.exe', '-u', '-d', '0'] 

PRODUCER_ADMIN_NAME = os.environ.get('PRODUCER_ADMIN_NAME', 'admin')
PRODUCER_ANALYSIS_NAME = os.environ.get('PRODUCER_ANALYSIS_NAME', 'analysis')

MZREFINER_NXFWFV_ID = os.environ.get('REFINE_MZML_WFVID')

# django
ALLOWED_HOSTS = os.environ.get('HOST_DOMAIN', KANTELEHOST).split(',')
SECRET_KEY = os.environ.get('SECRET_KEY')
DEBUG = True
# Build paths inside the project like this: os.path.join(BASE_DIR, ...)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_AUTO_FIELD = 'django.db.models.AutoField'
CSRF_COOKIE_SECURE = False # enforces HTTPS on cookie
# 10 MB data upload instead of 2.5 max - for results viewer
DATA_UPLOAD_MAX_MEMORY_SIZE = 10485760 
#SESSION_COOKIE_SECURE = True
#X_FRAME_OPTIONS = 'DENY'
#SECURE_CONTENT_TYPE_NOSNIFF = True
#SECURE_BROWSER_XSS_FILTER = False


# Application definition

LOGIN_REDIRECT_URL = '/'
LOGOUT_REDIRECT_URL = '/'
APPEND_SLASH = True


INSTALLED_APPS = [
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'home.apps.HomeConfig',
    'datasets.apps.DatasetsConfig',
    'rawstatus.apps.RawstatusConfig',
    'jobs.apps.JobsConfig',
    'corefac.apps.CorefacConfig',
    'analysis.apps.AnalysisConfig',
    'dashboard.apps.DashboardConfig',
    'staffpage.apps.StaffPageConfig',
    'mstulos.apps.MSTulosConfig',
]

MIDDLEWARE = [
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    #'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
]

ROOT_URLCONF = 'kantele.urls'

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [(os.path.join(BASE_DIR, 'templates')), ],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    },
]

WSGI_APPLICATION = 'kantele.wsgi.application'


# Database
# https://docs.djangoproject.com/en/1.11/ref/settings/#databases
DB_PASS = os.environ.get('DB_PASS')
DB_USER = os.environ.get('DB_USER')
DB_HOST = os.environ.get('DB_HOST')
DB_NAME = os.environ.get('DB_NAME')

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql',
        'NAME': DB_NAME,
        'USER': DB_USER,
        'PASSWORD': DB_PASS,
        'HOST': DB_HOST,
        'PORT': 5432,
        'ATOMIC_REQUESTS': True,
        'TEST': {'MIGRATE': False},
    }
}

if os.environ.get('CLEAN_DB_INIT'):
    MIGRATION_MODULES = {
            'analysis': None,
            'corefac': None,
            'dashboard': None,
            'datasets': None,
            'home': None,
            'jobs': None,
            'mstulos': None,
            'rawstatus': None,
            }

# Password validation
# https://docs.djangoproject.com/en/1.11/ref/settings/#auth-password-validators

AUTH_PASSWORD_VALIDATORS = [
    {
        'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator',
    },
]


# Internationalization

LANGUAGE_CODE = 'en-us'

TIME_ZONE = 'UTC'

USE_I18N = True

USE_L10N = True

USE_TZ = True


# Static files (CSS, JavaScript, Images)

STATIC_URL = '/static/'
STATIC_ROOT = 'static/'
LOGIN_URL = '/login'
SESSION_COOKIE_EXPIRE = 1800
SESSION_EXPIRE_AT_BROWSER_CLOSE = True
SESSION_SAVE_EVERY_REQUEST = True
