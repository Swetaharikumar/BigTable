from http.server import HTTPServer, BaseHTTPRequestHandler
import json
from parser_util import UrlParser
import consts as const


# table_names = {"tables" : []}
# table_meta_data = {}
# mem_table = {}
# get_function_types = ['List', 'GetInfo', 'do_DELETE']


class MyHandler(BaseHTTPRequestHandler):
    def _set_response(self, code):
        self.send_response(code)
        self.send_header('Content-type', 'application/json')
        self.end_headers()


        

    def do_GET(self):
        

        #data = None
        parser_get_type_obj = UrlParser('get')
        dict_return = parser_get_type_obj.parse(self.path)

        #perform on the dict_return
        
        #List Tables
        if(dict_return["function_name"] == const.get_function_types[0]):
            data = const.table_names
            data_json = json.dumps(data)
            self._set_response(200)
            self.wfile.write(data_json.encode("utf8"))

  
        


        #Table Get Info
        elif dict_return["function_name"] == const.get_function_types[1]:
            table_info = {}
            if dict_return["table_name"] in const.table_names["tables"]:
                table_info = const.table_meta_data[dict_return["table_name"]]
                data_json = json.dumps(table_info)
                self._set_response(200)
                self.wfile.write(data_json.encode("utf8"))
    
           
      
            else:
                self._set_response(404)


   

        

    def do_POST(self):
        # example: reading content from HTTP request
        data = None
        content_length = self.headers['content-length']
        if content_length != None:
            content_length = int(content_length)
            post_data = self.rfile.read(content_length)
        

        parser_post_type_obj = UrlParser('post')
        dict_return = parser_post_type_obj.parse(self.path)


        
        
        #Create a table
        if(dict_return["function_name"] == const.post_function_types[0]):
            my_dict = {}

            try:
                my_dict = json.loads(post_data)
            except:
                self._set_response(400)


            if my_dict != {}:
                exists = True if my_dict['name'] in const.table_meta_data else False


                if exists == False:
                    const.table_names["tables"].append(my_dict['name'])
                    const.table_meta_data[my_dict['name']] = my_dict
                    const.mem_table[my_dict['name']] = []
                    self._set_response(200)
                else:
                    self._set_response(409)


                
            

    def do_DELETE(self):
        path_list = self.path.split('/')
        if path_list[1] == 'api' and path_list[2] == 'tables':
            if path_list[3] in const.table_names["tables"]:
                const.table_names["tables"].remove(path_list[3])
                del const.table_meta_data[path_list[3]]
                self._set_response(200)
            else:
                self._set_response(404)







if __name__ == "__main__":
    server_address = ("localhost", 8080)
    handler_class = MyHandler
    server_class = HTTPServer

    httpd = HTTPServer(server_address, handler_class)
    print("sample server running...")

    try: 
        httpd.serve_forever()
    except KeyboardInterrupt: pass

    httpd.server_close()

