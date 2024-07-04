import os
import pandas as pd

class MarkdownConverter():
    def __init__(self):
        self.markdown = ''
        self.add_pipes = lambda value: '| '+value+' '
        self.cur_dir = os.getcwd()
        self.task1_csv = os.path.join(self.cur_dir, 'output/task1_m/lookup_table.csv')
        self.task2_dir = os.path.join(self.cur_dir, 'output/task2_m/csv/')
        self.out_task1_opt = ''

    def write_markdown(self, file_path):
        with open(file_path, 'w') as file:
            file.write(self.markdown)

    def markdown_line(self, iter):
        return ''.join(list(map(self.add_pipes, iter)))+'|\n'

    def convert_csv_file(self, csv_path):
        csv_file = pd.read_csv(csv_path)
        headers = list(csv_file.keys())
        
        self.markdown += self.markdown_line(headers)
        self.markdown += self.markdown_line(['---']*len(headers))

        for _, row in csv_file.iterrows():
            self.markdown += self.markdown_line(row)
        self.markdown += '\n'

    
    # load from task 1 and 2 csv results    
    def convert_csv_opt1(self, file_output):
        self.convert_csv_file(self.task1_csv)
        for hero_csv in os.listdir(self.task2_dir):
            self.convert_csv_file(os.path.join(self.task2_dir, hero_csv))
        self.out_task1_opt = os.path.join(self.cur_dir, file_output)
        self.write_markdown(file_output)
    
mc = MarkdownConverter()
mc.convert_csv_opt1('./output/task1_opt/task1_optional.md')
