def invertedIndex(data):
	output = {}
	for word in data:
		try:
			try:
				output[word[0]][word[1]] += 1
			except KeyError:
				output[word[0]][word[1]] = 1
		except KeyError:
			output[word[0]] = {}
			output[word[0]][word[1]] = 1
	return output