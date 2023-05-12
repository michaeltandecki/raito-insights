update-requirements:
	echo "test"

export-environment:
	conda env export > environment.yml 

report:
	python src/report.py