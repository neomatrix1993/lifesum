# Implementation of the program using threading, celery and redis.
import threading
import requests
import json
import time
import os,sys, signal
import heapq, redis
from tasks import processResponse, sortDict, flushDB, setupRedis, saveFiles
start_time = time.time()

# DONT CHANGE THIS NUMBER -- number_of_threads
number_of_threads = 100 # for 100 threads
test_threads = 100 # for testing purpose
iterations = 1 # max = 14000

# python run.py fresh ---> will reset the offset file with intial values
# if file is not there, it will create a new file while writing into it
fresh = False;
if len(sys.argv) > 1:
	if sys.argv[1] == "fresh":
		fresh = True

min_offset = 658330693 # actual 658330693
max_offset = 732158492 # actual 732158492

offset_diff = (max_offset - min_offset) / number_of_threads

url = 'https://api.lifesum.com/v1/foodipedia/foodstats'
starting_offset = [] # initial array of starting offset, calculated by number of threads and range of values
next_offset = [] # next_offset value for each thread stored here
threads = []
food_id = {}
food_cat = {}

# data stored in external files for simplification
offset_filename = 'offset.txt'
food_id_filename = 'food_id.txt'
food_cat_filename = 'food_cat.txt'

# load existing offset file, so it can resume from where it left.
if os.path.exists(offset_filename) and not fresh:
	print "Opening existing offset file...", offset_filename
	target = open(offset_filename, 'r')
	data = target.read()
	next_offset = [x.strip("[], ") for x in data.split()]
	next_offset = map(int, next_offset)
	print next_offset
	print "Resuming from the loaded offset file..."
	target.close()  
	for i in range(0, number_of_threads):
		starting_offset.append(i)
		starting_offset[i] = min_offset + i*offset_diff
else:
	for i in range(0, number_of_threads):
		starting_offset.append(i)
		next_offset.append(i)
		starting_offset[i] = min_offset + i*offset_diff
	next_offset = starting_offset

if fresh:
	flushDB.delay()
	print "Initialized blank food id and cat txt files..."
	json.dump(food_id, open(food_id_filename, 'w'))
	json.dump(food_cat, open(food_cat_filename, 'w'))
else:
	setupRedis()
def saveOffset():
	json.dump(next_offset, open(offset_filename, 'w'))

# called by each thread
def get_url(u):
	sys.stdout.write("\nThread Initiated: %s " % threads[u])
	if u<number_of_threads-1:
		offset_limit = starting_offset[u+1]
	else:
		offset_limit = max_offset
	while next_offset[u] <= offset_limit and flag.isSet(): #temp offset for three iterations
	# for i in range(0, iterations, 1):
		response = requests.get(url, params={'offset': next_offset[u]}).json()
		next_offset[u] = response['meta']['next_offset']
		processResponse.delay(response)
		sys.stdout.write("\nThread %s Updated Offset %s " % (u, next_offset[u]))
		sys.stdout.flush()
	saveOffset()

flag = threading.Event()
flag.set()

for u in range(0, test_threads):
	try:
		t = threading.Thread(target=get_url, args = (u, ))
		t.daemon = True
		threads.append(t)
		t.start()
	except Exception as ex:
		print "Error: ", ex

r = redis.Redis()
r1 = redis.Redis(db=1)

def signal_handler(signal, frame):
	print('You pressed Ctrl+C!')
	flag.clear()
	time.sleep(2)
	saveOffset()
	print "\n\nWait a few seconds, retrieving top ids:...\n(might take upto a minute)"
	print "Make sure the celery worker is running, or this will just keep waiting."
	top_food_id = sortDict.delay()
	top_food_id = top_food_id.get()
	print "Top 10 food_ids: "
	for i in top_food_id[0]:
		print " %s : %s " %(i, r.get(i)) 
	print "Top 5 food_ids: "
	for i in top_food_id[1]:
		print " %s : %s " %(i, r1.get(i))
	print "\n\nSaved Offset File: ---->", offset_filename
	sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)
signal.pause()

for t in threads:
    t.join()

print("\n--- %s seconds ---" % (time.time() - start_time))
