def invertedIndex(data):
	filename, words = data.split(' ')[0], data.split(' ')[1:]
	mapOutput = []
	for w in words:
		mapOutput.append((w.strip('.!?@#$%^&*()<>{}:" '), filename))
	return mapOutput
