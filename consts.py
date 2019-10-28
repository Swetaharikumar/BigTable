import Operation as op


table_names = {"tables" : []}
table_meta_data = {}
manifest = {"ssindex": {}}
# post_function_types = ['Create Table', 'Insert Cell', "Set Max Entries"]
# get_function_types = ['List Table', 'Get Table Info', 'Retrieve a Cell', 'Retrieve Cells', 'Retrieve a Row']
# del_function_types = ['Delete Table']

get_function_types = ['Retrieve a Cell', 'Retrieve Cells', 'Retrieve a Row']
post_function_types = ['Create','Insert']
del_function_types = ['Delete']







manifest_filename = "manifest.txt"
WAL_filename = "WAL.txt"
WAL = []
operation = op.Operation()
