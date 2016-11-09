from celery import Celery
import requests, json
import os
import redis, heapq

app = Celery('tasks', backend='redis://', broker='amqp://localhost//')

# r and r1 are two databases connections for storing food_id and food_cat
r = redis.Redis()
r1 = redis.Redis(db=1)

food_id_filename = 'food_id.txt'
food_cat_filename = 'food_cat.txt'

food_id = {}
food_cat = {}

@app.task
def flushDB():
	r.flushdb()
	r1.flushdb()
	setupRedis()

# setup existing values into redis for resuming functionality 
def setupRedis():	
	if os.path.exists(food_id_filename):
		print "Resuming from the loaded food_id.txt file..."
		food_id = eval(open(food_id_filename, 'r').read())
	if os.path.exists(food_cat_filename):
		print "Resuming from the loaded food_cat.txt file..."
		food_cat = eval(open(food_cat_filename, 'r').read())
	for key in food_id:
		r.set(str(key), food_id[key])
	for key1 in food_cat:
		r1.set(str(key1), food_cat[key1])

# process the api response from all the threads
@app.task
def processResponse(response):
	for s in response['response']:
		value_id = r.get(s['food_id'])
		value_cat = r1.get(s['food__category_id'])
		if not value_id:
			r.set(s['food_id'], 1)
			# food_id[str(s['food_id'])] = 1
		else:
			r.set(s['food_id'], int(value_id) + 1)
			# food_id[str(s['food_id'])] = int(value_id) + 1
		if not value_cat:
			r1.set(s['food__category_id'], 1)
			# food_cat[str(s['food__category_id'])] = 1
		else:
			r1.set(s['food__category_id'], int(value_cat) + 1)

# function to save the ids file for future use
def saveFiles():
	json.dump(food_id, open(food_id_filename, 'w'))
	json.dump(food_cat, open(food_cat_filename, 'w'))
	print "Saved ---> ", food_id_filename, food_cat_filename

# sort the ids dictionary and retreive top n elements
@app.task
def sortDict():
	food_id = {}
	food_cat = {}

	# extra step tp remove celery-tasks objects getting stored in redis
	for key in r.scan_iter(match='celery*'):
		r.delete(key)
	# iterating over redis keys to fetch all keys and sort top 10
	for key in r.scan_iter():
		food_id[key] = r.get(key)
	# for database 2 (food_cat)
	for key in r1.scan_iter(match='celery*'):
		r1.delete(key)
	for key in r1.scan_iter():
		food_cat[key] = r1.get(key)

	# save food_ids into external file
	json.dump(food_id, open(food_id_filename, 'w'))
	json.dump(food_cat, open(food_cat_filename, 'w'))

	# using heapq to get top n elements
	top_food_id = heapq.nlargest(10, food_id, key=food_id.get)
	top_cat_id = heapq.nlargest(5, food_cat, key=food_cat.get)

	# This will print on the celery monitor
	print "Top food id: "
	for i in top_food_id:
		print i, " : ",  food_id[i]
	print "Top cat id: "
	for i in top_cat_id:
		print i, " : ",  food_cat[i]
	return top_food_id, top_cat_id
