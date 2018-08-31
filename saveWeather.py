import boto3
import urllib2, urllib, json
import time
import uuid

woeids = {
	'Calgary': 8775,
	'Edmonton': 8676,
	'Red Deer': 8734,
	'Lethbridge': 8820,
	'Medicine Hat': 8623
}

def lambda_handler(err, res):
	client = boto3.client('dynamodb')
	
	for city in woeids:
		print('city', city)
		weatherData = get_weather(woeids[city])
		log = formatLog(weatherData, city)
		print('log', log)
		client.put_item(TableName = 'weather', Item=log)
		time.sleep(0.5)
	return weatherData

def get_weather(woeid):
	baseurl = "https://query.yahooapis.com/v1/public/yql?"
	yql_query = "select item.condition, astronomy from weather.forecast where woeid={} AND u='c'".format(woeid)
	yql_url = baseurl + urllib.urlencode({'q':yql_query}) + "&format=json" + "&u=c"
	result = urllib2.urlopen(yql_url).read()
	data = json.loads(result)
	print(data['query'])

	return data['query']

def formatLog(weatherData, city):
	print(city, weatherData)
	for key in weatherData:
		print(key)
		print(weatherData[key])
	log = {
		'id': {'S': str(uuid.uuid1())},
		'city': {'S': city},
		'epochTime': {'N': str(time.time())},
		'date': {'S': weatherData['results']['channel']['item']['condition']['date']},
		'text': {'S': weatherData['results']['channel']['item']['condition']['text']},
		'code': {'S': weatherData['results']['channel']['item']['condition']['code']},
		'temp': {'S': weatherData['results']['channel']['item']['condition']['temp']},
		'sunrise': {'S': weatherData['results']['channel']['astronomy']['sunrise']},
		'sunset': {'S': weatherData['results']['channel']['astronomy']['sunset']},
	}
	return log
