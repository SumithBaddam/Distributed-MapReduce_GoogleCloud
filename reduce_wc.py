def wordCount(data):
	output = {}
	for word in data:
		try:
			output[word[0]] += 1
		except KeyError:
			output[word[0]] = 1
	return output
