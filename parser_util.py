
import consts as const

class UrlParser():
	def __init__(self, httpType):
		# command = ['get', 'post', delete']
		self.httpType = httpType
	
		self.url_length = 4
		self.return_dict = {
			"is_404" : False,
			"table_name" : None,
			"cell_name" : None,
			"function_name" : None
		}

		

	def parse(self, url):
		"""
		Input : url string example : '/api/tables/+'
		Output : 
			{
			 is_404 : 404 or not
			 table_name :
			 cell_name :
			 function_name :
			}
		"""

		url = url.split('/')[1:] #ignore empty start list[0]
		if url[0] == 'api' and url[1] == 'tables' and len(url) <=self.url_length:

			#Check if HttpType is get, post or delete
			if(self.httpType.lower() == 'get'):


				# List tables if
				if(len(url) == 2): 
					self.return_dict["function_name"] = const.get_function_types[0]

         
                # Get Table Info
				elif(len(url) == 3):
					self.return_dict["function_name"] = const.get_function_types[1]
					self.return_dict["table_name"] = url[2]


                
				else:
					self.return_dict["is_404"] = True

			elif self.httpType.lower() == 'post':

				if(len(url) == 2):
					self.return_dict["function_name"] = const.post_function_types[0]
					
					





			elif self.httpType.lower() == 'delete':
				pass


		else:
			self.return_dict["is_404"] = True


		return self.return_dict


"""

-big_table_router_handler.py
-server.py
-router.py

-utils
----url_parser.py
----insert_function_finder.py

-database
----dB
----timestamps
----keys
----cache
----config.dat


-apis
----cache_handler.py
----insert_apis.py
----create_apis.py
----delete_apis.py
----search_apis.py
----update_apis.py
----shard_apis.py
----replication_apis.py
----coherence.py



"""







