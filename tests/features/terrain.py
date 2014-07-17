from lettuce import *
import requests

@before.all
def say_hello():
    print "Hello there!"
    print "Lettuce will start to run tests right now..."

@before.each_step
def tell_me_world_response(step):
    #print('--------------------------------------------------')
    #print("running step %r, defined at %s" % (
        #step.sentence,
        #step.defined_at.file))

    #if hasattr(world, 'wqpserver'):
        #print('world.wqpserver: ' + world.wqpserver)
    
    if False:
        if hasattr(world, 'response'):
            print('world.response:')
            print(world.response.url)
            print(str(world.response.status_code) + ' ' + world.response.reason)
            for header in world.response.headers:
                print(header + ':' + world.response.headers[header])
            print

    if hasattr(world, 'datarows'):
        print('len(world.datarows): ' + str(len(world.datarows)))
        
