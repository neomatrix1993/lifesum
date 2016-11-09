# Implementing thr program with just basic threading and queue.
import Queue
import threading
import requests
import json
import time
import os,sys, signal
import heapq
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

food_id = {}
food_cat = {}

# To resume the count of the food ids and categories.
if os.path.exists(food_id_filename) and not fresh:
	print "Resuming from the loaded food_id.txt file..."
	food_id = eval(open(food_id_filename, 'r').read())
if os.path.exists(food_cat_filename) and not fresh:
	print "Resuming from the loaded food_cat.txt file..."
	food_cat = eval(open(food_cat_filename, 'r').read())

q = Queue.Queue()

def saveFiles():
	print "Saving files values to file..."
	json.dump(next_offset, open(offset_filename, 'w'))
	json.dump(food_id, open(food_id_filename, 'w'))
	json.dump(food_cat, open(food_cat_filename, 'w'))

	print "Saved ---> ", offset_filename, food_id_filename, food_cat_filename

# called by each thread
def get_url(q, u):
	sys.stdout.write("\nThread Initiated: %s " % threads[u])
	if u<number_of_threads-1:
		offset_limit = starting_offset[u+1]
	else:
		offset_limit = max_offset
	while next_offset[u] <= offset_limit and flag.isSet(): 
	# for i in range(0, iterations, 1): # test iterations
		response = requests.get(url, params={'offset': next_offset[u]}).json()
		next_offset[u] = response['meta']['next_offset']
		q.put(response)
		sys.stdout.write("\nThread %s Updated Offset %s " % (u, next_offset[u]))
        sys.stdout.flush()

def processResponse(response):
	for s in response['response']:
		if s['food_id'] not in food_id.keys():
			food_id[s['food_id']] = 1
		else:
			food_id[s['food_id']] += 1
		if s['food__category_id'] not in food_cat.keys():
			food_cat[s['food__category_id']] = 1
		else:
			food_cat[s['food__category_id']] += 1

# def main():
	# init 100 threads at once
# try:
flag = threading.Event()
flag.set()

for u in range(0, test_threads):
	try:
		t = threading.Thread(target=get_url, args = (q, u))
		t.daemon = True
		threads.append(t)
		t.start()
	except Exception as ex:
		print "Error: ", ex

def sortDict(A, n):
	B = heapq.nlargest(n, A, key=A.get)
	for i in B:
		print i, " : ",  A[i]

def signal_handler(signal, frame):
	print('You pressed Ctrl+C!')
	flag.clear()
	time.sleep(2)
	saveFiles()
	print "Top 10 food ids:"
	sortDict(food_id, 10)
	print "\nTop 5 food cat:"
	sortDict(food_cat, 5)
	sys.exit(0)
signal.signal(signal.SIGINT, signal_handler)


# Process Queue
while True:
	if threading.active_count() == 1 and q.empty():
		break
	if not q.empty():
		s = q.get()
		sys.stdout.write("\nQueue Size: %s ResCode: %s " % (q.qsize(), s['meta']['code']))
		sys.stdout.flush()
		processResponse(s)
		q.task_done() # to notify that q item is processed


'''Multi Process Queue, speeds up the queue processing flow, uncomment to use this.'''
# for i in range(10):
# 	try:
# 		w = threading.Thread(target=processQueue)
# 		w.daemon = True
# 		w.start()
# 	except Exception as ex:
# 		print "Error: ", ex

q.join()
print "Queue is empty, complete...."

# write to file.
saveFiles()

for t in threads:
    t.join()

print("\n--- %s seconds ---" % (time.time() - start_time))