


test: lint
	@NODE_ENV=test ./node_modules/.bin/mocha -b --reporter spec --recursive

lint:
	./node_modules/.bin/jshint .

test-cov: lint
	@NODE_ENV=testing \
	  ./node_modules/.bin/istanbul cover \
		./node_modules/.bin/_mocha -- test \
	 -u exports --R spec	--timeout 10000 --recursive
	echo "html coverage report has been created at ./coverage/lcov-report/index.html"

test-codecov.io:
	@NODE_ENV=test ./node_modules/.bin/istanbul cover \
	./node_modules/mocha/bin/_mocha --report lcovonly -- -R spec && \
		cat ./coverage/lcov.info | ./node_modules/codecov.io/bin/codecov.io.js --verbose

.PHONY: test
