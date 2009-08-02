clean:
	find . -name '*~' -exec rm -fv {} \;
	find . -name '*.pyc' -exec rm -fv {} \;
wipe:
	rm -f {mini,}data/pickle.jar
stats:
	python recommend.py stats | less
production:
	time python recommend.py production >/dev/null
