from rdfizer import semantify
import sys
import getopt



'''
	Function executed when the current file is executed as a script, instead of being
	executed as a Python package in another script.

	When executing the current file as a script in the terminal, the following flags
	are accepted:

	-h (python3 -m rdfizer -h): prompts the correct use of semantify.py as a script
	-c (python3 -m rdfizer -c <config_file>): executes the program as a script with
		with the <config_file> parameter as the path to the configuration file to be
		used
	--config_file (python3 semantify.py --config_file <config_file>): same behaviour
		as -c flag

	Parameters
	----------
	Nothing

	Returns
	-------
	Nothing

'''

def main():
	argv = sys.argv[1:]
	try:
		opts, args = getopt.getopt(argv, 'hc:', 'config_file=')
	except getopt.GetoptError:
		print('python3 -m rdfizer -c <config_file>')
		sys.exit(1)
	for opt, arg in opts:
		if opt == '-h':
			print('python3 -m rdfizer -c <config_file>')
			sys.exit()
		elif opt == '-c' or opt == '--config_file':
			config_path = arg

		semantify(config_path)


if __name__ == "__main__":
	main()