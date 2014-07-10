import sys, os, inspect

class Utils:

    def setup_target_path(self):
        '''
        This method modifies sys.os.path to include ../pywqp, where the tested
        resources live.
        '''
        # TODO fix this to accept arbitrary path and to do sanity check
        # establish reference to directory that contains what we're testing,
        # and put that into the classpath
        currframe = inspect.currentframe()
        myfile = os.path.abspath(inspect.getfile(currframe))
        # must get rid of frame reference to avoid nasty cycles
        del currframe
        mydir = os.path.split(myfile)[0]
        topdir = os.path.split(mydir)[0]
        victim_folder = os.path.join(topdir, 'pywqp')

        if victim_folder not in sys.path:
            sys.path.insert(0, victim_folder)

