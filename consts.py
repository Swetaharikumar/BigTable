import Operation as op


table_names = {"tables" : []}
table_meta_data = {}
manifest = {}
post_function_types = ['Create Table', 'Insert Cell', "Set Max Entries"]
get_function_types = ['List Table', 'Get Table Info', 'Retrieve a Cell', 'Retrieve Cells', 'Retrieve a Row']
del_function_types = ['Delete Table']
manifest_filename = "manifest.txt"
WAL_filename = "WAL.txt"
WAL = []
operation = op.Operation()
