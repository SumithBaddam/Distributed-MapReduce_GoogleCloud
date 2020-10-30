def wordCount(data):
	words = data.split(' ')
	mapOutput = []
	for w in words:
		mapOutput.append((w.strip('.!?@#$%^&*()<>{}:" '), 1))
	return mapOutput
