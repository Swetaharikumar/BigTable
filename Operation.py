import json
import consts as const
# pip install BTrees
from BTrees.OOBTree import OOBTree
import pickle


class Operation:
    def __init__(self):
        self.mem_table = {}
        self.ssTable = {}
        self.max_entries = 100
        self.entries = 0
        self.files = []
        self.filenum = 0

    def insert(self, table_name, post_data):
        if table_name not in const.table_names["tables"]:
            return 404
        cell = {}
        try:
            cell = json.loads(post_data)
        except:
            return 400
        if cell == {}:
            return 400
        if not self.find_column_family_and_column(table_name, cell["column_family"], cell["column"]):
            return 400
        if table_name not in self.mem_table:
            self.mem_table[table_name] = {}
        if cell["column_family"] not in self.mem_table[table_name]:
            self.mem_table[table_name][cell["column_family"]] = {}
        if cell["column"] not in self.mem_table[table_name][cell["column_family"]]:
            self.mem_table[table_name][cell["column_family"]][cell["column"]] = OOBTree()
        t = self.mem_table[table_name][cell["column_family"]][cell["column"]]
        if cell["row"] not in t:
            t.update({cell["row"]: {"row": cell["row"], "data": cell["data"]}})
        else:
            t[cell["row"]]["data"] += cell["data"]
            while len(t[cell["row"]]["data"]) > 5:
                t[cell["row"]]["data"].pop(0)
        self.entries = self.entries + 1
        if self.entries == self.max_entries:
            self.spill_to_disk()
        return 200

    def retrieve(self, table_name, get_data):
        if table_name not in const.table_names["tables"]:
            return {"success": False, "success_code": 404}
        input = {}
        try:
            input = json.loads(get_data)
        except:
            return {"success": False, "success_code": 400}
        if input == {}:
            return {"success": False, "success_code": 400}

        if not self.find_column_family_and_column(table_name, input["column_family"], input["column"]):
            return {"success": False, "success_code": 400}

        if table_name in self.mem_table:
            if input["column_family"] in self.mem_table[table_name]:
                if input["column"] in self.mem_table[table_name][input["column_family"]]:
                    if input["row"] in self.mem_table[table_name][input["column_family"]][input["column"]]:
                        return {"success": True,
                                "data": self.mem_table[table_name][input["column_family"]][input["column"]][
                                    input["row"]]}

        # Not found in memory
        for file_name in self.files:
            with open(file_name, 'rb') as file:
                sstable = pickle.load(file)
                if table_name in sstable:
                    if input["column_family"] in sstable[table_name]:
                        if input["column"] in sstable[table_name][input["column_family"]]:
                            if input["row"] in sstable[table_name][input["column_family"]][input["column"]]:
                                return {"success": True,
                                        "data": sstable[table_name][input["column_family"]][input["column"]][
                                            input["row"]]}

        # if input["row"] not in self.ssTable:
        #     return {"success": False, "success_code": 404}
        # files = self.ssTable[input["row"]]
        # for file in files:
        #     with open(file) as json_file:
        #         data = json.load(json_file)
        #         for row in data[table_name]:
        #             if row["column_family"] == input["column_family"] and row["column"] == input["column"] and row[
        #                     "row"] == input["row"]:
        #                 return {"success": True, "data": row}
        #
        return {"success": False, "success_code": 409}

    def retrieve_cells(self, table_name, get_data):
        if table_name not in const.table_names["tables"]:
            return {"success": False, "success_code": 404}
        input = {}
        try:
            input = json.loads(get_data)
        except:
            return {"success": False, "success_code": 400}
        if input == {}:
            return {"success": False, "success_code": 400}

        if not self.find_column_family_and_column(table_name, input["column_family"], input["column"]):
            return {"success": False, "success_code": 400}

        if table_name in self.mem_table:
            if input["column_family"] in self.mem_table[table_name]:
                if input["column"] in self.mem_table[table_name][input["column_family"]]:
                    t = self.mem_table[table_name][input["column_family"]][input["column"]]
                    rows = list(t.values(input["row_from"], input["row_to"]))
                    if not rows == []:
                        return {"success": True, "data": {"rows": rows}}

        for file_name in self.files:
            with open(file_name, 'rb') as file:
                sstable = pickle.load(file)
                if table_name in sstable:
                    if input["column_family"] in sstable[table_name]:
                        if input["column"] in sstable[table_name][input["column_family"]]:
                            t = sstable[table_name][input["column_family"]][input["column"]]
                            rows = list(t.values(input["row_from"], input["row_to"]))
                            if not rows == []:
                                return {"success": True, "data": {"rows": rows}}

        return {"success": False, "success_code": 409}

    def set_max_entries(self, post_data):
        my_dict = {}
        try:
            my_dict = json.loads(post_data)
        except:
            return 400
        if 'memtable_max' not in my_dict:
            return 400
        new_entries = my_dict['memtable_max']

        if not isinstance(new_entries, int):
            return 400
        self.max_entries = new_entries
        if self.max_entries >= self.entries:
            self.spill_to_disk()
        return 200

    def find_column_family_and_column(self, table_name, column_family_name, column_name):
        for column_family in const.table_meta_data[table_name]["column_families"]:
            if column_family["column_family_key"] == column_family_name:
                if column_name in column_family["columns"]:
                    return True
        return False

    def spill_to_disk(self):
        file_name = 'data' + str(self.filenum + 1) + '.txt'
        pickle.dump(self.mem_table, open("save.p", "wb"))
        # entry = 0
        with open(file_name, 'wb') as outfile:
            pickle.dump(self.mem_table, outfile)
        #     for table_name in self.mem_table:
        #         for column_family in self.mem_table[table_name]:
        #             for column in self.mem_table[table_name][column_family]:
        #                 for row in self.mem_table[table_name][column_family][column]:
        #                     if table_name not in self.ssTable:
        #                         self.ssTable.update({table_name: {}})
        #                     if column_family not in self.ssTable[table_name]:
        #                         self.ssTable[table_name].update({column_family: {}})
        #                     if column not in self.ssTable[table_name][column_family]:
        #                         self.ssTable[table_name][column_family].update({column: {}})
        #                     if row not in self.ssTable[table_name][column_family][column]:
        #                         self.ssTable[table_name][column_family][column].update({row: []})
        #                     self.ssTable[table_name][column_family][column][row].append({"file_name": file_name, "entry": entry})
        #                     outfile.write(json.dumps(
        #                         {"table": table_name, "column_family": column_family, "column": column, "row": row,
        #                          "data": self.mem_table[table_name][column_family][column][row]}))
        #                     outfile.write('\n')
        #                     entry += 1

        self.mem_table = {}
        self.entries = 0
        self.files.append(file_name)
        self.filenum += 1
