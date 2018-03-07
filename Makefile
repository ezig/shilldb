test:
	find ./tests -name '*.rkt' -exec raco make '{}' \;
	find ./tests -name '*.rkt' -exec raco test '{}' \;

