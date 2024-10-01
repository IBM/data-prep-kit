import os
import numpy as np
from jinja2 import Environment, FileSystemLoader
import pyarrow as pa
import plotly.graph_objects as go
from plotly.io import to_html
from pathlib import Path

class Plot:
    '''
    Plot class implements the generation of frequency distribution plots of the various components of the profiler report.
    Given a pyarrow table and a column name, it generates the corresponding plot.
    '''
    def __init__(self, table, column_name):
        self.table = table
        self.column_name = column_name
        self.column_data = self._get_column_data()

    def _get_column_data(self):
        return self.table[self.column_name].to_numpy()

    def generate_distribution_plot(self):
        data = self.column_data
        fig = go.Figure()
        fig.add_trace(go.Histogram(x=data, nbinsx=len(np.unique(data)), opacity=0.7, marker=dict(color='blue', line=dict(width=1, color='black'))))
        fig.update_layout(
            width=500,  
            height=300,  
            title=dict(
                text=f'Distribution of {self.column_name}',
                font=dict(size=14)  
            ),
            xaxis=dict(
                title='Value',
                title_font=dict(size=12),  
                tickfont=dict(size=10)  
            ),
            yaxis=dict(
                title='Frequency',
                title_font=dict(size=12), 
                tickfont=dict(size=10) 
            ),
            bargap=0.1
        )
        return to_html(fig, full_html=False)


class Report:
    '''
    Generates the report containing the distribution of various syntactic and semantic components.
    '''
    def __init__(self, template_file: str):
        path = Path(template_file)
        directory = path.parent 
        file_name = path.name  
        self.env = Environment(loader=FileSystemLoader(directory))
        self.template = self.env.get_template(file_name)
        self.data = {}
        self.data['title'] = 'Profiler Report'
        self.data['heading'] = 'Syntactic and Semantic Profile'
        self.data['description'] = 'This report presents the detailed profiling report of the input dataset.'

    def add_metric(self, metric_id, name, graph_html=None):
        if 'metrics' not in self.data:
            self.data['metrics'] = []
        self.data['metrics'].append({
            'id': metric_id,
            'name': name,
            'graph_html': graph_html
        })

    def render(self):
        return self.template.render(self.data)

    def save(self, output_file):
        output = self.render()
        with open(output_file, 'w') as f:
            f.write(output)
        print(f"HTML file generated: {output_file}")




# # Usage example
# if __name__ == "__main__":
#     data = {
#         'column1': [1, 2, 2, 3, 3, 3, 4, 4, 4, 4]
#     }
#     table = pa.table(data)
#     plot = Plot(table, 'column1')
#     plot_html = plot.generate_distribution_plot()
#     report = Report('template.html')
#     report.add_metric('metric1', 'Metric 1', 'Details about Metric 1.', plot_html)
#     report.save('output.html')