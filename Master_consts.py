import Master_operations as mop


table_names = {"tables" : []}
table_info = {}
server_load_dict = {"tablet1":0, "tablet2" :0, "tablet3": 0}
server_table_dict = {"tablet1":[], "tablet2" :[], "tablet3" : []}
tablets_info = {"tablet1":{"hostname": "localhost", "port": 8010}, "tablet2" : {"hostname": "localhost", "port": 8011}, "tablet3":{"hostname": "localhost", "port": 8012}}
post_function_types = ['Create Table', 'Insert Cell', "Set Max Entries",'Open lock']
get_function_types = ['List Table', 'Get Table Info', 'Retrieve a Cell', 'Retrieve Cells', 'Retrieve a Row']
del_function_types = ['Delete Table', 'Close lock']
locks = {}
check_start = False
running_tablets = ["tablet1", "tablet2"]

master_operation = mop.MasterOperations()
