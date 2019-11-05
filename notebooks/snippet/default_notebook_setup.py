## Not all libraries support reloads. This might break things.
## https://ipython.org/ipython-doc/3/config/extensions/autoreload.html
## use %autoreload 1 and %aimport project for selective reloads

%reload_ext autoreload
%autoreload 1

## Load environment variables from our .env file
#%load_ext dotenv
#%dotenv

# Add the jupyter home directory to the python path
# to find our project code base for imports
import sys
sys.path.append('/home/jovyan')
